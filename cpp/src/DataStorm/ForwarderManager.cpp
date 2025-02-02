// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/ForwarderManager.h>

using namespace std;
using namespace DataStormI;

ForwarderManager::ForwarderManager(const shared_ptr<Ice::ObjectAdapter>& adapter) :
    _adapter(adapter), _nextId(0)
{
}

shared_ptr<Ice::ObjectPrx>
ForwarderManager::add(const shared_ptr<Forwarder>& forwarder)
{
    lock_guard<mutex> lock(_mutex);
    ostringstream os;
    os << _nextId++;
    _forwarders.emplace(os.str(), forwarder);
    try
    {
        return _adapter->createProxy({ os.str(), "forwarders"});
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
        return nullptr;
    }
}

void
ForwarderManager::remove(const Ice::Identity& id)
{
    lock_guard<mutex> lock(_mutex);
    _forwarders.erase(id.name);
}

bool
ForwarderManager::ice_invoke(Ice::ByteSeq inEncaps, Ice::ByteSeq&, const Ice::Current& current)
{
    shared_ptr<Forwarder> forwarder;
    {
        lock_guard<mutex> lock(_mutex);
        auto p = _forwarders.find(current.id.name);
        if(p == _forwarders.end())
        {
            throw Ice::ObjectNotExistException(__FILE__, __LINE__, current.id, current.facet, current.operation);
        }
        forwarder = p->second;
    }
    forwarder->forward(inEncaps, current);
    return true;
}
