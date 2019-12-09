// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/LookupSessionI.h>
#include <DataStorm/LookupSessionManager.h>
#include <DataStorm/ConnectionManager.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TraceUtil.h>

using namespace std;
using namespace DataStormI;
using namespace DataStormContract;

namespace
{

class NodeForwarderI : public Node, public enable_shared_from_this<NodeForwarderI>
{
public:

    NodeForwarderI(shared_ptr<Instance> instance, shared_ptr<NodePrx> node) :
        _instance(move(instance)),
        _node(move(node))
    {
    }

    virtual void tryCreateSession(shared_ptr<NodePrx> node, const Ice::Current& current) override
    {
        try
        {
            _node->tryCreateSessionAsync(node);
        }
        catch(const Ice::ObjectAdapterDeactivatedException&)
        {
        }
        catch(const Ice::CommunicatorDestroyedException&)
        {
        }
    }

    virtual void createSession(shared_ptr<NodePrx> node,
                               shared_ptr<SubscriberSessionPrx> subscriberSession,
                               const Ice::Current& current) override
    {
        try
        {
            if(updateSessionProxy(node, subscriberSession, current))
            {
                _node->createSessionAsync(node,
                                          subscriberSession,
                                          nullptr,
                                          [subscriberSession](auto ex)
                                          {
                                              try
                                              {
                                                  subscriberSession->destroyAsync();
                                              }
                                              catch(const Ice::ObjectAdapterDeactivatedException&)
                                              {
                                              }
                                              catch(const Ice::CommunicatorDestroyedException&)
                                              {
                                              }
                                          });
            }
        }
        catch(const Ice::ObjectAdapterDeactivatedException&)
        {
        }
        catch(const Ice::CommunicatorDestroyedException&)
        {
        }
    }

    virtual void ackCreateSession(shared_ptr<NodePrx> node,
                                  shared_ptr<PublisherSessionPrx> publisherSession,
                                  const Ice::Current& current) override
    {
        try
        {
            if(updateSessionProxy(node, publisherSession, current))
            {
                _node->ackCreateSessionAsync(node,
                                             publisherSession,
                                             nullptr,
                                             [publisherSession](auto ex)
                                             {
                                                 try
                                                 {
                                                     publisherSession->destroyAsync();
                                                 }
                                                 catch(const Ice::ObjectAdapterDeactivatedException&)
                                                 {
                                                 }
                                                 catch(const Ice::CommunicatorDestroyedException&)
                                                 {
                                                 }
                                             });
            }
        }
        catch(const Ice::ObjectAdapterDeactivatedException&)
        {
        }
        catch(const Ice::CommunicatorDestroyedException&)
        {
        }
    }

private:

    template<typename T> bool updateSessionProxy(shared_ptr<NodePrx>& node,
                                                 shared_ptr<T>& session,
                                                 const Ice::Current& current)
    {
        assert(node);
        if(session->ice_getEndpoints().empty() && session->ice_getAdapterId().empty())
        {
            auto s = _instance->getLookupSessionManager()->getSession(node);
            if(!s)
            {
                Ice::uncheckedCast<T>(current.con->createProxy(session->ice_getIdentity()))->destroyAsync();
                return false;
            }
            session = s->getSessionForwarder(session);
        }
        return true;
    }

    const shared_ptr<Instance> _instance;
    const shared_ptr<NodePrx> _node;
};

}

LookupSessionI::LookupSessionI(shared_ptr<Instance> instance,
                               shared_ptr<NodePrx> node,
                               shared_ptr<Ice::Connection> connection) :
    _instance(move(instance)),
    _traceLevels(_instance->getTraceLevels()),
    _node(move(node)),
    _connection(move(connection))
{
    _lookup = Ice::uncheckedCast<LookupPrx>(_connection->createProxy({ "Lookup", "DataStorm" }));

    if(_node->ice_getEndpoints().empty() && _node->ice_getAdapterId().empty())
    {
        auto bidirNode = Ice::uncheckedCast<NodePrx>(_connection->createProxy(_node->ice_getIdentity()));
        auto forwarder = make_shared<NodeForwarderI>(_instance, bidirNode);
        _publicNode = Ice::uncheckedCast<NodePrx>(_instance->getObjectAdapter()->add(forwarder, _node->ice_getIdentity()));
    }
    else
    {
        _publicNode = _node;
    }

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "created lookup session (peer = `" << _publicNode << "')";
    }
}

void
LookupSessionI::destroy()
{
    if(_publicNode != _node)
    {
        try
        {
            _instance->getObjectAdapter()->remove(_publicNode->ice_getIdentity());
        }
        catch(const Ice::ObjectAdapterDeactivatedException&)
        {
        }
    }

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "destroyed lookup session (peer = `" << _publicNode << "')";
    }
}

std::shared_ptr<SessionPrx>
LookupSessionI::forwarder(const std::shared_ptr<SessionPrx>& session) const
{
    auto id = session->ice_getIdentity();
    auto proxy = _instance->getObjectAdapter()->createProxy({ id.name + "-" + _node->ice_getIdentity().name,
                                                              id.category });
    return Ice::uncheckedCast<SessionPrx>(proxy);
}
