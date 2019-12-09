// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/LookupSessionManager.h>
#include <DataStorm/ConnectionManager.h>
#include <DataStorm/Instance.h>
#include <DataStorm/ForwarderManager.h>
#include <DataStorm/LookupSessionI.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/Timer.h>
#include <DataStorm/TraceUtil.h>

using namespace std;
using namespace DataStormI;
using namespace DataStormContract;

namespace
{

class SessionForwarderI : public Ice::Blobject
{
public:

    SessionForwarderI(shared_ptr<LookupSessionManager> lookupSessionManager) :
        _lookupSessionManager(move(lookupSessionManager))
    {
    }

    virtual bool
    ice_invoke(Ice::ByteSeq inEncaps, Ice::ByteSeq&, const Ice::Current& curr)
    {
        auto pos = curr.id.name.find('-');
        if(pos != string::npos && pos < curr.id.name.length())
        {
            auto s = _lookupSessionManager->getSession(curr.id.name.substr(pos + 1));
            if(s)
            {
                auto id = Ice::Identity { curr.id.name.substr(0, pos), curr.id.category };
                s->getLookup()->ice_identity(id)->ice_invokeAsync(curr.operation, curr.mode, inEncaps, curr.ctx);
                return true;
            }
        }
        throw Ice::ObjectNotExistException(__FILE__, __LINE__, curr.id, curr.facet, curr.operation);
    }

private:

    shared_ptr<LookupSessionManager> _lookupSessionManager;
};

}


LookupSessionManager::LookupSessionManager(shared_ptr<Instance> instance) :
    _instance(move(instance)),
    _traceLevels(_instance->getTraceLevels())
{
}

void
LookupSessionManager::init()
{
    auto forwarder = [self=shared_from_this()](Ice::ByteSeq e, const Ice::Current& c) { self->forward(e, c); };
    _forwarder = Ice::uncheckedCast<LookupPrx>(_instance->getCollocatedForwarder()->add(move(forwarder)));

    _sessionForwarder = make_shared<SessionForwarderI>(shared_from_this());

    auto communicator = _instance->getCommunicator();
    auto connectTo = communicator->getProperties()->getProperty("DataStorm.Node.ConnectTo");
    if(!connectTo.empty())
    {
        connect(Ice::uncheckedCast<LookupPrx>(communicator->stringToProxy("DataStorm/Lookup:" + connectTo)));
    }
}

void
LookupSessionManager::create(const shared_ptr<NodePrx>& node, const shared_ptr<Ice::Connection>& connection)
{
    unique_lock<mutex> lock(_mutex);
    if(_connectedTo.find(node->ice_getIdentity()) != _connectedTo.end())
    {
        return;
    }

    auto p = _sessions.find(node->ice_getIdentity());
    if(p != _sessions.end())
    {
        p->second->destroy();
        _sessions.erase(p);
    }

    _sessions.emplace(node->ice_getIdentity(), make_shared<LookupSessionI>(_instance, node, connection));

    auto self = shared_from_this();
    _instance->getConnectionManager()->add(node, connection, [=](auto connection, auto ex)
    {
        self->destroySession(node);
    });
}

void
LookupSessionManager::announceTopicReader(const string& topic,
                                          const shared_ptr<NodePrx>& node,
                                          const shared_ptr<Ice::Connection>& connection) const
{
    unique_lock<mutex> lock(_mutex);
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        if(connection)
        {
            out << "topic reader `" << topic << "' announced (peer = `" << node << "')";
        }
        else
        {
            out << "announcing topic reader `" << topic << "' (peer = `" << node << "')";
        }
    }
    _exclude = connection;
    auto p = _sessions.find(node->ice_getIdentity());
    if(p != _sessions.end())
    {
        _forwarder->announceTopicReader(topic, p->second->getPublicNode());
    }
    else
    {
        _forwarder->announceTopicReader(topic, node);
    }
}

void
LookupSessionManager::announceTopicWriter(const string& topic,
                                          const shared_ptr<NodePrx>& node,
                                          const shared_ptr<Ice::Connection>& connection) const
{
    unique_lock<mutex> lock(_mutex);
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        if(connection)
        {
            out << "topic writer `" << topic << "' announced (peer = `" << node << "')";
        }
        else
        {
            out << "announcing topic writer `" << topic << "' (peer = `" << node << "')";
        }
    }
    _exclude = connection;
    auto p = _sessions.find(node->ice_getIdentity());
    if(p != _sessions.end())
    {
        _forwarder->announceTopicWriter(topic, p->second->getPublicNode());
    }
    else
    {
        _forwarder->announceTopicWriter(topic, node);
    }
}

void
LookupSessionManager::announceTopics(const StringSeq& readers,
                                     const StringSeq& writers,
                                     const shared_ptr<NodePrx>& node,
                                     const shared_ptr<Ice::Connection>& connection) const
{
    unique_lock<mutex> lock(_mutex);
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        if(connection)
        {
            if(!readers.empty())
            {
                out << "topic readers `" << readers << "' announced (peer = `" << node << "')";
            }
            if(!writers.empty())
            {
                out << "topic writers `" << writers << "' announced (peer = `" << node << "')";
            }
        }
        else
        {
            if(!readers.empty())
            {
                out << "announcing topic readers `" << readers << "' (peer = `" << node << "')";
            }
            if(!writers.empty())
            {
                out << "announcing topic writers `" << writers << "' (peer = `" << node << "')";
            }
        }
    }
    _exclude = connection;
    auto p = _sessions.find(node->ice_getIdentity());
    if(p != _sessions.end())
    {
        _forwarder->announceTopics(readers, writers, p->second->getPublicNode());
    }
    else
    {
        _forwarder->announceTopics(readers, writers, node);
    }
}

shared_ptr<LookupSessionI>
LookupSessionManager::getSession(const shared_ptr<NodePrx>& node) const
{
    unique_lock<mutex> lock(_mutex);
    auto p = _sessions.find(node->ice_getIdentity());
    if(p != _sessions.end())
    {
        return p->second;
    }
    return nullptr;
}

shared_ptr<LookupSessionI>
LookupSessionManager::getSession(const string& name) const
{
    unique_lock<mutex> lock(_mutex);
    auto p = _sessions.find({ name, "" });
    if(p != _sessions.end())
    {
        return p->second;
    }
    return nullptr;
}

void
LookupSessionManager::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    for(const auto& session : _sessions)
    {
        if(session.second->getConnection() != _exclude)
        {
            session.second->getLookup()->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
        }
    }
    for(const auto& lookup : _connectedTo)
    {
        if(lookup.second.second->ice_getCachedConnection() != _exclude)
        {
            lookup.second.second->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
        }
    }
}

void
LookupSessionManager::destroy() const
{
    _instance->getCollocatedForwarder()->remove(_forwarder->ice_getIdentity());
}

void
LookupSessionManager::connect(const shared_ptr<LookupPrx>& lookup)
{
    try
    {
        lookup->createSessionAsync(_instance->getNode()->getProxy(),
                                   [=](auto node)
                                   {
                                       connected(node, lookup);
                                   },
                                   [=](auto ex)
                                   {
                                       disconnected(nullptr, lookup);
                                   });
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
        disconnected(nullptr, lookup);
    }
    catch(const Ice::CommunicatorDestroyedException&)
    {
        disconnected(nullptr, lookup);
    }
}

void
LookupSessionManager::connected(const shared_ptr<NodePrx>& node, const shared_ptr<LookupPrx>& lookup)
{
    unique_lock<mutex> lock(_mutex);
    if(_sessions.find(node->ice_getIdentity()) != _sessions.end())
    {
        return;
    }

    auto connection = lookup->ice_getCachedConnection();
    connection->setAdapter(_instance->getObjectAdapter());
    _instance->getConnectionManager()->add(lookup, connection, [=](auto connection, auto ex)
    {
        disconnected(node, lookup);
    });
    _connectedTo.emplace(node->ice_getIdentity(), make_pair(node, lookup));

    try
    {
        lookup->announceTopicsAsync(_instance->getTopicFactory()->getTopicReaderNames(),
                                    _instance->getTopicFactory()->getTopicWriterNames(),
                                    _instance->getNode()->getProxy());
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
    catch(const Ice::CommunicatorDestroyedException&)
    {
    }

    Ice::EndpointSeq endpoints;
    for(const auto& p : _connectedTo)
    {
        auto endpts = p.second.first->ice_getEndpoints();
        endpoints.insert(endpoints.end(), endpts.begin(), endpts.end());
    }
    _instance->getNode()->updatePublicProxy(_instance->getNode()->getProxy()->ice_endpoints(endpoints));

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "established lookup session (peer = `" << node << "')";
    }
}

void
LookupSessionManager::disconnected(const shared_ptr<NodePrx>& node, const shared_ptr<LookupPrx>& lookup)
{
    unique_lock<mutex> lock(_mutex);
    if(node != nullptr)
    {
        if(_traceLevels->session > 0)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << "disconnected lookup session (peer = `" << node << "')";
        }
        _connectedTo.erase(node->ice_getIdentity());
        connect(lookup);
    }
    else
    {
        _instance->getTimer()->schedule(5000ms, [=] { connect(lookup); });
    }
}

void
LookupSessionManager::destroySession(const shared_ptr<NodePrx>& node)
{
    unique_lock<mutex> lock(_mutex);
    auto p = _sessions.find(node->ice_getIdentity());
    if(p == _sessions.end())
    {
        return;
    }

    p->second->destroy();
    _sessions.erase(p);
}
