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

class SessionI;
class CallbackExecutor;

class SessionManager
{
public:

    SessionManager(const std::shared_ptr<CallbackExecutor>&);

    void add(const std::shared_ptr<SessionI>&, std::shared_ptr<Ice::Connection>);
    void remove(const std::shared_ptr<SessionI>&, std::shared_ptr<Ice::Connection>);
    void remove(std::shared_ptr<Ice::Connection>);
    void destroy();

private:

    std::mutex _mutex;
    std::map<std::shared_ptr<Ice::Connection>, std::set<std::shared_ptr<SessionI>>> _connections;
    std::shared_ptr<CallbackExecutor> _executor;
};

}
