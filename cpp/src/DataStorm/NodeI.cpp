// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/NodeI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/SessionI.h>
#include <DataStorm/LookupSessionManager.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/TraceUtil.h>
#include <DataStorm/CallbackExecutor.h>

using namespace std;
using namespace DataStormI;
using namespace DataStormContract;

namespace
{

class ServantLocatorI : public Ice::ServantLocator
{
public:

    ServantLocatorI(shared_ptr<NodeI> node, shared_ptr<LookupSessionManager> lookupSessionManager) :
        _node(move(node)),
        _lookupSessionManager(move(lookupSessionManager))
    {
    }

    virtual shared_ptr<Ice::Object> locate(const Ice::Current& current, shared_ptr<void>&) override
    {
        auto servant = _node->getSession(current.id);
        if(servant)
        {
            return servant;
        }
        else if(current.id.name.length() > 2) // <id>-<node UUID> or <id>-<node UUID> name
        {
            return _lookupSessionManager->getSessionForwarder();
        }
        return nullptr;
    }

    virtual void finished(const Ice::Current&, const Ice::ObjectPtr&, const shared_ptr<void>&) override
    {
        _node->getInstance()->getCallbackExecutor()->flush();
    }

    virtual void deactivate(const string&) override
    {
    }

private:

    const shared_ptr<NodeI> _node;
    const shared_ptr<LookupSessionManager> _lookupSessionManager;
};

template<typename T> void
updateNodeAndSession(shared_ptr<NodePrx>& node, shared_ptr<T>& session, const Ice::Current& current)
{
    if(current.con)
    {
        assert(current.con->getAdapter());
        if(node->ice_getEndpoints().empty() && node->ice_getAdapterId().empty())
        {
            node = Ice::uncheckedCast<NodePrx>(current.con->createProxy(node->ice_getIdentity()));
        }
        if(session->ice_getEndpoints().empty() && session->ice_getAdapterId().empty())
        {
            session = Ice::uncheckedCast<T>(current.con->createProxy(session->ice_getIdentity()));
        }
    }
}

}

NodeI::NodeI(const shared_ptr<Instance>& instance) :
    _instance(instance),
    _nextSubscriberSessionId(0),
    _nextPublisherSessionId(0)
{
}

NodeI::~NodeI()
{
    assert(_subscribers.empty());
    assert(_publishers.empty());
}

void
NodeI::init()
{
    auto self = shared_from_this();
    auto forwarder = [self=shared_from_this()](Ice::ByteSeq e, const Ice::Current& c) { self->forward(e, c); };
    _subscriberForwarder = Ice::uncheckedCast<SubscriberSessionPrx>(_instance->getCollocatedForwarder()->add(forwarder));
    _publisherForwarder = Ice::uncheckedCast<PublisherSessionPrx>(_instance->getCollocatedForwarder()->add(forwarder));
    try
    {
        auto adapter = _instance->getObjectAdapter();
        _proxy = Ice::uncheckedCast<NodePrx>(adapter->addWithUUID(self));
        _publicProxy = _proxy;

        auto servantLocator = make_shared<ServantLocatorI>(self, _instance->getLookupSessionManager());
        adapter->addServantLocator(servantLocator, "s");
        adapter->addServantLocator(servantLocator, "p");
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
}

void
NodeI::destroy(bool ownsCommunicator)
{
    unique_lock<mutex> lock(_mutex);
    if(!ownsCommunicator)
    {
        //
        // Close the connections associated with the session if we don't own the communicator.
        //
        for(const auto& p : _subscribers)
        {
            auto c = p.second->getConnection();
            if(c)
            {
                c->close(Ice::ConnectionClose::Gracefully);
            }
        }
        for(const auto& p : _publishers)
        {
            auto c = p.second->getConnection();
            if(c)
            {
                c->close(Ice::ConnectionClose::Gracefully);
            }
        }
    }
    _subscribers.clear();
    _publishers.clear();
    _subscriberSessions.clear();
    _publisherSessions.clear();
    _instance->getCollocatedForwarder()->remove(_subscriberForwarder->ice_getIdentity());
    _instance->getCollocatedForwarder()->remove(_publisherForwarder->ice_getIdentity());
    _instance->getCallbackExecutor()->destroy();
}

void
NodeI::createSubscriberSession(const shared_ptr<NodePrx>& subscriber)
{
    try
    {
        unique_lock<mutex> lock(_mutex);
        subscriber->tryCreateSessionAsync(_publicProxy);
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
    catch(const Ice::CommunicatorDestroyedException&)
    {
    }
}

void
NodeI::createPublisherSession(const shared_ptr<NodePrx>& publisher)
{
    try
    {
        publisher->ice_getConnectionAsync(
            [=, self=shared_from_this()](auto connection)
            {
                if(connection && !connection->getAdapter())
                {
                    connection->setAdapter(_instance->getObjectAdapter());
                }

                shared_ptr<SubscriberSessionI> session;
                try
                {
                    unique_lock<mutex> lock(_mutex);
                    session = createSubscriberSessionServant(publisher);
                    if(!session || session->getSession())
                    {
                        return; // Shutting down or already connected
                    }

                    publisher->createSessionAsync(_publicProxy,
                                                  session->getProxy<SubscriberSessionPrx>(),
                                                  nullptr,
                                                  [=](auto ex)
                                                  {
                                                    self->removeSubscriberSession(publisher, session, ex);
                                                  });
                }
                catch(const Ice::LocalException&)
                {
                    removeSubscriberSession(publisher, session, current_exception());
                }
            });
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
    catch(const Ice::CommunicatorDestroyedException&)
    {
    }
}

void
NodeI::tryCreateSession(shared_ptr<NodePrx> publisher, const Ice::Current& current)
{
    createPublisherSession(move(publisher));
}

void
NodeI::createSession(shared_ptr<NodePrx> subscriber,
                     shared_ptr<SubscriberSessionPrx> subscriberSession,
                     const Ice::Current& current)
{
    try
    {
        subscriber->ice_getConnectionAsync([=, self=shared_from_this()](auto connection) mutable
        {
            if(connection && !connection->getAdapter())
            {
                connection->setAdapter(_instance->getObjectAdapter());
            }

            shared_ptr<PublisherSessionI> session;
            try
            {
                unique_lock<mutex> lock(_mutex);
                session = createPublisherSessionServant(subscriber);
                if(!session || session->getSession())
                {
                    // TODO: XXX
                    return;
                }

                updateNodeAndSession(subscriber, subscriberSession, current);

                // Must be called before connected
                subscriber->ackCreateSessionAsync(_publicProxy, session->getProxy<PublisherSessionPrx>());

                subscriberSession->ice_getConnectionAsync([=](auto con)
                {
                    session->connected(subscriberSession, con, self->_instance->getTopicFactory()->getTopicWriters());
                },
                [=](auto ex)
                {
                    self->removePublisherSession(subscriber, session, ex);
                });
            }
            catch(const Ice::LocalException&)
            {
                removePublisherSession(subscriber, session, current_exception());
            }
        });
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
    catch(const Ice::CommunicatorDestroyedException&)
    {
    }
}

void
NodeI::ackCreateSession(shared_ptr<NodePrx> publisher,
                        shared_ptr<PublisherSessionPrx> publisherSession,
                        const Ice::Current& current)
{
    unique_lock<mutex> lock(_mutex);

    updateNodeAndSession(publisher, publisherSession, current);

    auto p = _subscribers.find(publisher->ice_getIdentity());
    assert(p != _subscribers.end());
    auto session = p->second;
    assert(!session->getSession());

    auto self = shared_from_this();
    publisherSession->ice_getConnectionAsync([=](auto con)
    {
        session->connected(publisherSession, current.con, self->_instance->getTopicFactory()->getTopicReaders());
    },
    [=](auto ex)
    {
        self->removeSubscriberSession(publisher, session, ex);
    });
}

void
NodeI::removeSubscriberSession(const shared_ptr<NodePrx>& node,
                               const shared_ptr<SubscriberSessionI>& session,
                               const exception_ptr& ex)
{
    unique_lock<mutex> lock(_mutex);
    if(!session->getSession() && session->getNode() == node)
    {
        auto p = _subscribers.find(session->getNode()->ice_getIdentity());
        if(p != _subscribers.end() && p->second == session)
        {
            _subscribers.erase(p);
            _subscriberSessions.erase(session->getProxy()->ice_getIdentity());
            session->destroyImpl(ex);
        }
    }
}

void
NodeI::removePublisherSession(const shared_ptr<NodePrx>& node,
                              const shared_ptr<PublisherSessionI>& session,
                              const exception_ptr& ex)
{
    unique_lock<mutex> lock(_mutex);
    if(!session->getSession() && session->getNode() == node)
    {
        auto p = _publishers.find(session->getNode()->ice_getIdentity());
        if(p != _publishers.end() && p->second == session)
        {
            _publishers.erase(p);
            _publisherSessions.erase(session->getProxy()->ice_getIdentity());
            session->destroyImpl(ex);
        }
    }
}

shared_ptr<Ice::Connection>
NodeI::getSessionConnection(const string& id) const
{
    auto session = getSession(Ice::stringToIdentity(id));
    if(session)
    {
        return session->getConnection();
    }
    else
    {
        return nullptr;
    }
}

shared_ptr<SessionI>
NodeI::getSession(const Ice::Identity& ident) const
{
    unique_lock<mutex> lock(_mutex);
    if(ident.category == "s")
    {
        auto p = _subscriberSessions.find(ident);
        if(p != _subscriberSessions.end())
        {
            return p->second;
        }
    }
    else if(ident.category == "p")
    {
        auto p = _publisherSessions.find(ident);
        if(p != _publisherSessions.end())
        {
            return p->second;
        }
    }
    return nullptr;
}

void
NodeI::updatePublicProxy(shared_ptr<NodePrx> prx)
{
    unique_lock<mutex> lock(_mutex);
    if(_proxy->ice_getEndpoints().empty() && _proxy->ice_getAdapterId().empty())
    {
        _publicProxy = prx;
    }
}

shared_ptr<SubscriberSessionI>
NodeI::createSubscriberSessionServant(const shared_ptr<NodePrx>& node)
{
    auto p = _subscribers.find(node->ice_getIdentity());
    if(p != _subscribers.end())
    {
        p->second->setNode(node);
        return p->second;
    }
    else
    {
        try
        {
            auto session = make_shared<SubscriberSessionI>(shared_from_this(), node);
            ostringstream os;
            os << ++_nextSubscriberSessionId;
            session->init(Ice::uncheckedCast<SessionPrx>(_instance->getObjectAdapter()->createProxy({ os.str(), "s" })));
            _subscribers.emplace(node->ice_getIdentity(), session);
            _subscriberSessions.emplace(session->getProxy()->ice_getIdentity(), session);
            return session;
        }
        catch(const Ice::ObjectAdapterDeactivatedException&)
        {
            return nullptr;
        }
    }
}

shared_ptr<PublisherSessionI>
NodeI::createPublisherSessionServant(const shared_ptr<NodePrx>& node)
{
    auto p = _publishers.find(node->ice_getIdentity());
    if(p != _publishers.end())
    {
        p->second->setNode(node);
        return p->second;
    }
    else
    {
        try
        {
            auto session = make_shared<PublisherSessionI>(shared_from_this(), node);
            ostringstream os;
            os << ++_nextPublisherSessionId;
            session->init(Ice::uncheckedCast<SessionPrx>(_instance->getObjectAdapter()->createProxy({ os.str(), "p" })));
            _publishers.emplace(node->ice_getIdentity(), session);
            _publisherSessions.emplace(session->getProxy()->ice_getIdentity(), session);
            return session;
        }
        catch(const Ice::ObjectAdapterDeactivatedException&)
        {
            return nullptr;
        }
    }
}

void
NodeI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    lock_guard<mutex> lock(_mutex);
    if(current.id == _subscriberForwarder->ice_getIdentity())
    {
        for(const auto s : _subscribers)
        {
            shared_ptr<SessionPrx> session = s.second->getSession();
            if(session)
            {
                session->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
            }
        }
    }
    else
    {
        for(const auto s : _publishers)
        {
            shared_ptr<SessionPrx> session = s.second->getSession();
            if(session)
            {
                session->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
            }
        }
    }
}
