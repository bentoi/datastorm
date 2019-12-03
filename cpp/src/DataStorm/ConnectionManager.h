// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <DataStorm/Config.h>

#include <Ice/Ice.h>

namespace DataStormI
{

class CallbackExecutor;
class ForwarderManager;

class ConnectionManager
{
public:

    ConnectionManager(const std::shared_ptr<CallbackExecutor>&, const std::shared_ptr<ForwarderManager>&);

    void add(const std::shared_ptr<void>&,
             const std::shared_ptr<Ice::Connection>&,
             std::function<void(const std::shared_ptr<Ice::Connection>&, std::exception_ptr)>);

    template<typename T>
    std::shared_ptr<T> addForwarder(const std::shared_ptr<Ice::ObjectPrx>& prx,
                                    const std::shared_ptr<Ice::Connection>& con)
    {
        return Ice::uncheckedCast<T>(addForwarder(prx, con));
    }

    std::shared_ptr<Ice::ObjectPrx> addForwarder(const std::shared_ptr<Ice::ObjectPrx>&,
                                                 const std::shared_ptr<Ice::Connection>&);

    void remove(const std::shared_ptr<void>&, const std::shared_ptr<Ice::Connection>&);
    void remove(const std::shared_ptr<Ice::Connection>&);
    void removeForwarder(const std::shared_ptr<Ice::ObjectPrx>&, const std::shared_ptr<Ice::Connection>&);

    void destroy();

private:

    using Callback = std::function<void(const std::shared_ptr<Ice::Connection>&, std::exception_ptr)>;

    std::mutex _mutex;
    std::map<std::shared_ptr<Ice::Connection>, std::map<std::shared_ptr<void>, Callback>> _connections;
    std::shared_ptr<CallbackExecutor> _executor;
    std::shared_ptr<ForwarderManager> _forwarder;
};

}
