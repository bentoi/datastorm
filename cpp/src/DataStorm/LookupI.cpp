// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/Instance.h>
#include <DataStorm/LookupI.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/LookupSessionManager.h>
#include <DataStorm/NodeI.h>

using namespace std;
using namespace DataStormContract;
using namespace DataStormI;

LookupI::LookupI(const shared_ptr<Instance>& instance) : _instance(instance)
{
}

void
LookupI::announceTopicReader(string name, shared_ptr<NodePrx> proxy, const Ice::Current& current)
{
    if(proxy->ice_getEndpoints().empty() && proxy->ice_getAdapterId().empty())
    {
        proxy = Ice::uncheckedCast<NodePrx>(current.con->createProxy(proxy->ice_getIdentity()));
    }
    _instance->getLookupSessionManager()->announceTopicReader(name, proxy, current.con);
    _instance->getTopicFactory()->createSubscriberSession(name, proxy);
}

void
LookupI::announceTopicWriter(string name, shared_ptr<NodePrx> proxy, const Ice::Current& current)
{
    if(proxy->ice_getEndpoints().empty() && proxy->ice_getAdapterId().empty())
    {
        proxy = Ice::uncheckedCast<NodePrx>(current.con->createProxy(proxy->ice_getIdentity()));
    }
    _instance->getLookupSessionManager()->announceTopicWriter(name, proxy, current.con);
    _instance->getTopicFactory()->createPublisherSession(name, proxy);
}

void
LookupI::announceTopics(StringSeq readers, StringSeq writers, shared_ptr<NodePrx> proxy, const Ice::Current& current)
{
    if(proxy->ice_getEndpoints().empty() && proxy->ice_getAdapterId().empty())
    {
        proxy = Ice::uncheckedCast<NodePrx>(current.con->createProxy(proxy->ice_getIdentity()));
    }

    _instance->getLookupSessionManager()->announceTopics(readers, writers, proxy, current.con);
    for(auto name : readers)
    {
        _instance->getTopicFactory()->createSubscriberSession(name, proxy);
    }
    for(auto name : writers)
    {
        _instance->getTopicFactory()->createPublisherSession(name, proxy);
    }
}

shared_ptr<NodePrx>
LookupI::createSession(shared_ptr<NodePrx> node, const Ice::Current& current)
{
    _instance->getLookupSessionManager()->create(move(node), current.con);
    return _instance->getNode()->getProxy();
}
