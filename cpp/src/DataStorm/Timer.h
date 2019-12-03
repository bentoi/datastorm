// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <DataStorm/Config.h>

#include <memory>
#include <chrono>
#include <functional>
#include <thread>
#include <map>

namespace DataStormI
{

class Instance;

class Timer
{
public:

    Timer(std::shared_ptr<Instance>);

    void schedule(std::chrono::milliseconds, std::function<void()>);
    void destroy();

private:

    void runTimer();

    const std::shared_ptr<Instance> _instance;
    std::thread _thread;
    mutable std::mutex _mutex;
    std::condition_variable _cond;
    bool _destroyed;
    std::multimap<std::chrono::steady_clock::time_point, std::function<void()>> _timers;
};

}
