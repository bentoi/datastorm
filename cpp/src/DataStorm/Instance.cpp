// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/Instance.h>
#include <DataStorm/ConnectionManager.h>
#include <DataStorm/LookupI.h>
#include <DataStorm/TraceUtil.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/LookupSessionManager.h>
#include <DataStorm/Node.h>
#include <DataStorm/CallbackExecutor.h>
#include <DataStorm/Timer.h>

#include <IceUtil/UUID.h>

using namespace std;
using namespace DataStormI;

Instance::Instance(const shared_ptr<Ice::Communicator>& communicator) : _communicator(communicator), _shutdown(false)
{
    shared_ptr<Ice::Properties> properties = _communicator->getProperties();
    if(properties->getPropertyAsIntWithDefault("DataStorm.Node.Server", 1) > 0)
    {
        if(properties->getProperty("DataStorm.Node.Server.Endpoints").empty())
        {
            properties->setProperty("DataStorm.Node.Server.Endpoints", "tcp");
        }
        properties->setProperty("DataStorm.Node.Server.ThreadPool.SizeMax", "1");
        _adapter = _communicator->createObjectAdapter("DataStorm.Node.Server");
    }
    else
    {
        _adapter = _communicator->createObjectAdapter("");
    }

    if(properties->getPropertyAsIntWithDefault("DataStorm.Node.Multicast", 1) > 0)
    {
        properties->setProperty("DataStorm.Node.Multicast.Endpoints", "udp -h 239.255.0.1 -p 10000");
        properties->setProperty("DataStorm.Node.Multicast.ProxyOptions", "-d");
        properties->setProperty("DataStorm.Node.Multicast.ThreadPool.SizeMax", "1");
        _multicastAdapter = _communicator->createObjectAdapter("DataStorm.Node.Multicast");
    }
    else
    {
        _multicastAdapter = _communicator->createObjectAdapter("");
    }

    //
    // Create a collocated object adapter with a random name to prevent user configuration
    // of the adapter.
    //
    auto collocated = IceUtil::generateUUID();
    properties->setProperty(collocated + ".AdapterId", collocated);
    _collocatedAdapter = _communicator->createObjectAdapter(collocated);

    _collocatedForwarder = make_shared<ForwarderManager>(_collocatedAdapter, "forwarders");
    _collocatedAdapter->addDefaultServant(_collocatedForwarder, "forwarders");

    auto category = IceUtil::generateUUID();
    _publicForwarder = make_shared<ForwarderManager>(_adapter, category);
    _adapter->addDefaultServant(_publicForwarder, category);

    _executor = make_shared<CallbackExecutor>();
    _connectionManager = make_shared<ConnectionManager>(_executor, _publicForwarder);

    _traceLevels = make_shared<TraceLevels>(_communicator);
}

void
Instance::init()
{
    auto self = shared_from_this();

    _node = make_shared<NodeI>(self);
    _node->init();

    _timer = make_shared<Timer>(self);

    _lookupSessionManager = make_shared<LookupSessionManager>(self);
    _lookupSessionManager->init();

    _topicFactory = make_shared<TopicFactoryI>(self);

    auto lookupI = make_shared<LookupI>(self);
    auto lookup = _multicastAdapter->add(lookupI, {"Lookup", "DataStorm"});
    _lookup = Ice::uncheckedCast<DataStormContract::LookupPrx>(lookup->ice_collocationOptimized(false));
    _adapter->add(lookupI, {"Lookup", "DataStorm"});

    _adapter->activate();
    _collocatedAdapter->activate();
    _multicastAdapter->activate();
}

void
Instance::shutdown()
{
    unique_lock<mutex> lock(_mutex);
    _shutdown = true;
    _cond.notify_all();
    _topicFactory->shutdown();
}

bool
Instance::isShutdown() const
{
    unique_lock<mutex> lock(_mutex);
    return _shutdown;
}

void
Instance::checkShutdown() const
{
    unique_lock<mutex> lock(_mutex);
    if(_shutdown)
    {
        throw DataStorm::NodeShutdownException();
    }
}

void
Instance::waitForShutdown() const
{
    unique_lock<mutex> lock(_mutex);
    _cond.wait(lock, [&]() { return _shutdown; }); // Wait until shutdown is called
}

void
Instance::destroy(bool ownsCommunicator)
{
    _timer->destroy();

    if(ownsCommunicator)
    {
        _communicator->destroy();
    }
    else
    {
        _adapter->destroy();
        _collocatedAdapter->destroy();
        _multicastAdapter->destroy();
    }
    _node->destroy(ownsCommunicator);
}
