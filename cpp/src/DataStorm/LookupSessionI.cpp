// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/LookupSessionI.h>
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

    NodeForwarderI(shared_ptr<NodePrx> node, shared_ptr<ConnectionManager> connectionManager) :
        _node(move(node)),
        _connectionManager(move(connectionManager))
    {
    }

    void createSubscriberSessionAsync(shared_ptr<NodePrx> node,
                                      shared_ptr<PublisherSessionPrx> session,
                                      function<void (const shared_ptr<SubscriberSessionPrx>&)> response,
                                      function<void (exception_ptr)> exception,
                                      const Ice::Current& current)
    {
        assert(node && session);
        session = forward(session, current.con);
        _node->createSubscriberSessionAsync(node,
                                            session,
                                            [self=shared_from_this(), response=move(response), session, connection=current.con](auto ssession)
                                            {
                                                ssession = self->forward(ssession, self->_node->ice_getCachedConnection());

                                                self->_connectionManager->add(session,
                                                                              self->_node->ice_getCachedConnection(),
                                                                              [session](auto, auto)
                                                                              {
                                                                                  session->destroyAsync();
                                                                              });
                                                if(ssession)
                                                {
                                                    self->_connectionManager->add(ssession,
                                                                                  connection,
                                                                                  [ssession](auto, auto)
                                                                                  {
                                                                                      ssession->destroyAsync();
                                                                                  });
                                                }

                                                response(ssession);
                                            },
                                            move(exception));
    }

    void createPublisherSessionAsync(shared_ptr<NodePrx> node,
                                     shared_ptr<SubscriberSessionPrx> session,
                                     function<void (const shared_ptr<PublisherSessionPrx>&)> response,
                                     function<void (exception_ptr)> exception,
                                     const Ice::Current& current)
    {
        assert(node && session);
        session = forward(session, current.con);
        _node->createPublisherSessionAsync(node,
                                           session,
                                           [self=shared_from_this(), response=move(response), session, connection=current.con](auto psession)
                                           {
                                                psession = self->forward(psession, self->_node->ice_getCachedConnection());

                                                self->_connectionManager->add(session,
                                                                              self->_node->ice_getCachedConnection(),
                                                                              [session](auto, auto)
                                                                              {
                                                                                  session->destroyAsync();
                                                                              });

                                                if(psession)
                                                {
                                                    self->_connectionManager->add(psession,
                                                                                  connection,
                                                                                  [psession](auto, auto)
                                                                                  {
                                                                                      psession->destroyAsync();
                                                                                  });
                                                }

                                                response(psession);
                                           },
                                           move(exception));
    }

private:

    template<typename T> shared_ptr<T> forward(shared_ptr<T> proxy, std::shared_ptr<Ice::Connection> connection)
    {
        if(proxy && proxy->ice_getEndpoints().empty() && proxy->ice_getAdapterId().empty())
        {
            return _connectionManager->addForwarder<T>(proxy, connection);
        }
        else
        {
            return proxy;
        }
    }

    const shared_ptr<NodePrx> _node;
    const shared_ptr<ConnectionManager> _connectionManager;
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
        auto forwarder = make_shared<NodeForwarderI>(bidirNode, _instance->getConnectionManager());
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
LookupSessionI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    _lookup->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
}

void
LookupSessionI::destroy()
{
    if(_publicNode != _node)
    {
        _instance->getObjectAdapter()->remove(_publicNode->ice_getIdentity());
    }

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "destroyed lookup session (peer = `" << _publicNode << "')";
    }
}
