// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/ConnectionManager.h>
#include <DataStorm/ForwarderManager.h>
#include <DataStorm/CallbackExecutor.h>

using namespace std;
using namespace DataStormI;

ConnectionManager::ConnectionManager(const shared_ptr<CallbackExecutor>& executor,
                                     const shared_ptr<ForwarderManager>& forwarder) :
    _executor(executor), _forwarder(forwarder)
{
}

void
ConnectionManager::add(const shared_ptr<void>& object,
                       const shared_ptr<Ice::Connection>& connection,
                       function<void(const shared_ptr<Ice::Connection>&, exception_ptr)> callback)
{
    lock_guard<mutex> lock(_mutex);
    auto& objects = _connections[connection];
    if(objects.empty())
    {
        connection->setCloseCallback([=](const shared_ptr<Ice::Connection>& con)
        {
            remove(con);
        });
    }
    objects.emplace(move(object), move(callback));
}

shared_ptr<Ice::ObjectPrx>
ConnectionManager::addForwarder(const shared_ptr<Ice::ObjectPrx>& proxy, const shared_ptr<Ice::Connection>& connection)
{
    auto prx = _forwarder->add(connection->createProxy(proxy->ice_getIdentity()));
    add(prx, connection, [prx, forwarder=_forwarder](auto, auto) { forwarder->remove(prx->ice_getIdentity()); });
    return prx;
}

void
ConnectionManager::remove(const shared_ptr<void>& object, const shared_ptr<Ice::Connection>& connection)
{
    lock_guard<mutex> lock(_mutex);
    auto& objects = _connections[connection];
    if(objects.empty())
    {
        _connections.erase(connection);
    }
    objects.erase(object);
}

void
ConnectionManager::remove(const shared_ptr<Ice::Connection>& connection)
{
    map<shared_ptr<void>, Callback> objects;
    {
        lock_guard<mutex> lock(_mutex);
        auto p = _connections.find(connection);
        objects.swap(p->second);
        connection->setCloseCallback(nullptr);
        _connections.erase(p);
    }
    exception_ptr ex;
    try
    {
        connection->getInfo();
    }
    catch(const std::exception&)
    {
        ex = current_exception();
    }
    for(const auto& object : objects)
    {
        object.second(connection, ex);
    }
    _executor->flush();
}

void
ConnectionManager::removeForwarder(const shared_ptr<Ice::ObjectPrx>& prx, const shared_ptr<Ice::Connection>& connection)
{
    remove(prx, connection);
    _forwarder->remove(prx->ice_getIdentity());
}

void
ConnectionManager::destroy()
{
    lock_guard<mutex> lock(_mutex);
    for(const auto& connection : _connections)
    {
        connection.first->setCloseCallback(nullptr);
    }
    _connections.clear();
}
