// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/Timer.h>
#include <DataStorm/Instance.h>

using namespace std;
using namespace DataStormI;

Timer::Timer(shared_ptr<Instance> instance) :
    _instance(move(instance)),
    _thread(&Timer::runTimer, this),
    _destroyed(false)
{
}

void
Timer::schedule(chrono::milliseconds duration, function<void()> callback)
{
    lock_guard<mutex> lock(_mutex);
    _timers.emplace(chrono::steady_clock::now() + duration, callback);
    _cond.notify_one();
}

void
Timer::destroy()
{
    {
        lock_guard<mutex> lock(_mutex);
        _destroyed = true;
        _cond.notify_one();
    }
    _thread.join();
}

void
Timer::runTimer()
{
    vector<function<void()>> tasks;
    while(true)
    {
        {
            unique_lock<mutex> lock(_mutex);
            if(_destroyed)
            {
                return;
            }

            if(_timers.empty())
            {
                _cond.wait(lock, [this] { return !_timers.empty() || _destroyed; });
            }
            else
            {
                _cond.wait_until(lock,
                                 _timers.cbegin()->first,
                                 [this] { return _timers.cbegin()->first <= chrono::steady_clock::now() || _destroyed; });
                auto now = chrono::steady_clock::now();
                auto p = _timers.begin();
                for(; p != _timers.end() && p->first <= now; ++p)
                {
                    tasks.push_back(move(p->second));
                }
                _timers.erase(_timers.begin(), p);
            }
        }
        if(!tasks.empty())
        {
            for(auto& t : tasks)
            {
                try
                {
                    t();
                }
                catch(...)
                {
                    assert(false);
                }
            }
        }
        tasks.clear();
    }
}
