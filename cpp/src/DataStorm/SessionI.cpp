// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/SessionI.h>
#include <DataStorm/SessionManager.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TopicI.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/TraceUtil.h>

using namespace std;
using namespace DataStormInternal;
using namespace DataStormContract;

SessionI::SessionI(NodeI* parent, const shared_ptr<NodePrx>& node) :
    _instance(parent->getInstance()), _traceLevels(_instance->getTraceLevels()), _parent(parent), _node(node)
{
}

void
SessionI::init(const shared_ptr<SessionPrx>& prx)
{
    _proxy = prx;
    _id = prx->ice_getIdentity().category + "/" + prx->ice_getIdentity().name;
    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": created session (peer = `" << _node << "')";
    }
}

void
SessionI::announceTopics(TopicInfoSeq topics, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": announcing topics `" << topics << "'";
    }

    TopicInfoAndContentSeq ack;
    for(const auto& info : topics)
    {
        runWithTopics(info.name, [&](auto t)
        {
            t->attach(info.id, this, _session);
            ack.emplace_back(t->getTopicInfoAndContent(getLastId(t->getId())));
        });
    }
    if(!ack.empty())
    {
        _session->attachTopicsAsync(ack);
    }
}

void
SessionI::attachTopics(TopicInfoAndContentSeq topics, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": attaching topics `" << topics << "'";
    }

    for(const auto& info : topics)
    {
        runWithTopics(info.name, [&](auto t)
        {
            t->attach(info.id, this, _session);
            auto keys = t->attachKeysAndFilters(info.id, info.keys, info.filters, info.lastId, this, _session);
            auto filters = t->getFilterInfoSeq();
            if(!keys.empty() || !filters.empty())
            {
                _session->attachKeysAndFiltersAsync(t->getId(), getLastId(t->getId()), keys, filters);
            }
        });
    }
}

void
SessionI::detachTopic(long long int id, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": detaching topic `" << id << "'";
    }

    runWithTopics(id, [&](auto topic, auto subscribers) { topic->detach(id, this); });
}

void
SessionI::announceKeys(long long int id, KeyInfoSeq keys, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": announcing key `[" << keys << "]@" << id << "'";
    }

    runWithTopics(id, [&](auto topic, auto subscribers)
    {
        auto kAndF = topic->attachKeys(id, keys, 0, this, _session);
        if(!kAndF.first.empty() || !kAndF.second.empty())
        {
            _session->attachKeysAndFiltersAsync(topic->getId(), subscribers.getLastId(), kAndF.first, kAndF.second);
        }
    });
}

void
SessionI::announceFilter(long long int id, FilterInfo filter, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id <<  ": announcing filter `" << filter << '@' << id << "'";
    }

    runWithTopics(id, [&](auto topic, auto subscribers)
    {
        auto keys = topic->attachFilter(id, filter, 0, this, _session);
        if(!keys.empty())
        {
            _session->attachKeysAndFiltersAsync(topic->getId(), subscribers.getLastId(), keys, {});
        }
    });
}

void
SessionI::attachKeysAndFilters(long long int id,
                               long long int lastId,
                               KeyInfoAndSamplesSeq keys,
                               FilterInfoSeq filters,
                               const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": attaching keys and filters `([" << keys << "];[" << filters << "])@" << id << "'";
    }

    runWithTopics(id, [&](auto topic, auto subscribers)
    {
        auto samples = topic->attachKeysAndFilters(id, keys, filters, lastId, this, _session);
        if(!samples.empty())
        {
            Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(_session)->iAsync(topic->getId(), samples);
        }
    });
}

void
SessionI::detachKeys(long long int id, LongSeq keys, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": detaching key `[" << keys << "]@" << id << "'";
    }

    runWithTopics(id, [&](auto topic, auto subscribers)
    {
        for(auto key : keys)
        {
            auto k = subscribers.removeKey(key);
            for(auto subscriber : k.getSubscribers())
            {
                subscriber.first->detachKey(id, key, this, subscriber.second);
            }
        }
    });
}

void
SessionI::detachFilter(long long int id, long long int filter, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": detaching filter `" << filter << "@" << id << "'";
    }

    runWithTopics(id, [&](auto topic, auto subscribers)
    {
        auto f = subscribers.removeFilter(filter);
        for(auto subscriber : f.getSubscribers())
        {
            subscriber.first->detachFilter(id, filter, this, subscriber.second);
        }
    });
}

void
SessionI::connected(const shared_ptr<SessionPrx>& session,
                    const shared_ptr<Ice::Connection>& connection,
                    const TopicInfoSeq& topics)
{
    lock_guard<mutex> lock(_mutex);
    if(_connection)
    {
        return;
    }

    _connection = connection;
    if(!_connection->getAdapter())
    {
        _connection->setAdapter(_instance->getObjectAdapter());
    }
    _instance->getSessionManager()->add(this, connection);

    auto prx = connection->createProxy(session->ice_getIdentity())->ice_oneway();
    _session = Ice::uncheckedCast<SessionPrx>(prx);

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": session `" << _session->ice_getIdentity() << "' connected\n" << connection->toString();
    }

    if(!topics.empty())
    {
        _session->announceTopicsAsync(topics);
    }

    for(auto c : _connectedCallbacks)
    {
        c(_proxy);
    }
    _connectedCallbacks.clear();
}

void
SessionI::disconnected(exception_ptr ex)
{
    {
        lock_guard<mutex> lock(_mutex);
        if(!_session)
        {
            return;
        }

        if(_traceLevels->session > 0)
        {
            try
            {
                rethrow_exception(ex);
            }
            catch(const std::exception& e)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": session `" << _session->ice_getIdentity() << "' disconnected:\n";
                out << _connection->toString() << "\n";
                out << e.what();
            }
        }

        for(const auto& t : _topics)
        {
            runWithTopics(t.first, [&](auto topic, auto subscribers) { topic->detach(t.first, this, false); });
        }
        _topics.clear();

        _session = nullptr;
        _connection = nullptr;
    }

    //
    // Try re-connecting if we got disconnected.
    //
    // TODO: Improve retry logic.
    //
    if(!reconnect())
    {
        _proxy->destroy();
    }
}

void
SessionI::destroyImpl()
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": destroyed session";
    }

    _instance->getSessionManager()->remove(this, _connection);

    _session = nullptr;
    _connection = nullptr;

    for(const auto& t : _topics)
    {
        runWithTopics(t.first, [&](auto topic, auto subscribers) { topic->detach(t.first, this, false); });
    }
    _topics.clear();

    for(auto c : _connectedCallbacks)
    {
        c(nullptr);
    }
    _connectedCallbacks.clear();
}

void
SessionI::addConnectedCallback(function<void(shared_ptr<SessionPrx>)> callback)
{
    {
        lock_guard<mutex> lock(_mutex);
        if(!_session)
        {
            _connectedCallbacks.push_back(callback);
            return;
        }
    }
    callback(_proxy);
}

shared_ptr<SessionPrx>
SessionI::getSession() const
{
    lock_guard<mutex> lock(_mutex);
    return _session;
}

shared_ptr<SessionPrx>
SessionI::getSessionNoLock() const
{
    return _session;
}

long long int
SessionI::getLastId(long long int topic) const
{
    // Called within the topic synchronization
    // auto p = _topics.find(topic);
    // if(p != _topics.end())
    // {
    //     return p->second.getLastId();
    // }
    return -1;
}

void
SessionI::subscribe(long long id, TopicI* topic)
{
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": subscribed topic `" << id << "' to topic `" << topic << "'";
    }
    _topics[id].emplace(topic, TopicSubscribers());
}

void
SessionI::unsubscribe(long long id, TopicI* topic, bool remove)
{
    assert(_topics.find(id) != _topics.end());
    auto& subscribers = _topics.at(id).at(topic);
    for(auto k : subscribers.getKeys())
    {
        for(auto e : k.second.getSubscribers())
        {
            e.first->detachKey(id, k.first, this, e.second, false);
        }
    }
    for(auto f : subscribers.getFilters())
    {
        for(auto e : f.second.getSubscribers())
        {
            e.first->detachKey(id, f.first, this, e.second, false);
        }
    }
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": unsubscribed topic `" << id << "' from topic `" << topic << "'";
    }
    if(remove)
    {
        _topics.at(id).erase(topic);
        if(_topics.at(id).empty())
        {
            _topics.erase(id);
        }
    }
}

void
SessionI::disconnect(long long id, TopicI* topic)
{
    lock_guard<mutex> lock(_mutex); // Called by TopicI::destroy
    if(!_session)
    {
        return;
    }

    runWithTopic(id, topic, [&](auto) { unsubscribe(id, topic, true); });
}

void
SessionI::subscribeToKey(long long topic, long long int id, const shared_ptr<Key>& key, DataElementI* element)
{
    assert(_topics.find(topic) != _topics.end());
    auto& t = _topics.at(topic).at(element->getTopic());
    auto facet = element->getFacet();
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": subscribed key `" << id << '@' << topic << "' to `" << element << "'";
        if(!facet.empty())
        {
            out << " (facet=" << facet << ')';
        }
    }
    t.getKey(id, key)->add(element, facet);
}

void
SessionI::unsubscribeFromKey(long long topic, long long int id, DataElementI* element)
{
    assert(_topics.find(topic) != _topics.end());
    auto& t = _topics.at(topic).at(element->getTopic());
    auto k = t.getKey(id);
    if(k)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": unsubscribed key `" << id << '@' << topic << "' from `" << element << "'";
        }
        k->remove(element);
    }
}

void
SessionI::disconnectFromKey(long long topic, long long int id, DataElementI* element)
{
    lock_guard<mutex> lock(_mutex); // Called by DataElementI::destroy
    if(!_session)
    {
        return;
    }

    runWithTopic(topic, element->getTopic(), [&](auto) { unsubscribeFromKey(topic, id, element); });
}

void
SessionI::subscribeToFilter(long long topic, long long int id, const shared_ptr<Filter>& filter, DataElementI* element)
{
    assert(_topics.find(topic) != _topics.end());
    auto& t = _topics.at(topic).at(element->getTopic());
    auto facet = element->getFacet();
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": subscribed filter `" << id << '@' << topic << "' to `" << element << "'";
        if(!facet.empty())
        {
            out << " (facet=" << facet << ')';
        }
    }
    t.getFilter(id, filter)->add(element, facet);
}

void
SessionI::unsubscribeFromFilter(long long topic, long long int id, DataElementI* element)
{
    assert(_topics.find(topic) != _topics.end());
    auto& t = _topics.at(topic).at(element->getTopic());
    auto f = t.getFilter(id);
    if(f)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": unsubscribed filter `" << id << '@' << topic << "' from `" << element << "'";
        }
        f->remove(element);
    }
}

void
SessionI::disconnectFromFilter(long long topic, long long int id, DataElementI* element)
{
    lock_guard<mutex> lock(_mutex); // Called by DataElementI::destroy
    if(!_session)
    {
        return;
    }

    runWithTopic(topic, element->getTopic(), [&](auto) { unsubscribeFromFilter(topic, id, element); });
}

void
SessionI::runWithTopics(const std::string& name, function<void (const shared_ptr<TopicI>&)> fn)
{
    auto topics = getTopics(name);
    for(auto topic : topics)
    {
        unique_lock<mutex> l(topic->getMutex());
        _topicLock = &l;
        fn(topic);
        _topicLock = nullptr;
    }
}

void
SessionI::runWithTopics(long long int id, function<void (TopicI*, TopicSubscribers&)> fn)
{
    auto t = _topics.find(id);
    if(t != _topics.end())
    {
        auto tpcs = t->second;
        for(auto s : tpcs)
        {
            unique_lock<mutex> l(s.first->getMutex());
            _topicLock = &l;
            fn(s.first, s.second);
            _topicLock = nullptr;
        }
    }
}

void
SessionI::runWithTopic(long long int id, TopicI* topic, function<void (TopicSubscribers&)> fn)
{
    auto t = _topics.find(id);
    if(t != _topics.end())
    {
        auto s = t->second.find(topic);
        if(s != t->second.end())
        {
            unique_lock<mutex> l(s->first->getMutex());
            _topicLock = &l;
            fn(s->second);
            _topicLock = nullptr;
        }
    }
}

SubscriberSessionI::SubscriberSessionI(NodeI* parent, const shared_ptr<NodePrx>& node) :
    SessionI(parent, node)
{
}

void
SubscriberSessionI::destroy(const Ice::Current&)
{
    _parent->removeSubscriberSession(this);
}

vector<shared_ptr<TopicI>>
SubscriberSessionI::getTopics(const string& name) const
{
    return _instance->getTopicFactory()->getTopicReaders(name);
}

void
SubscriberSessionI::i(long long int id, DataSamplesSeq samplesSeq, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    auto communicator = _instance->getCommunicator();
    for(const auto& samples : samplesSeq)
    {
        runWithTopics(id, [&](auto topic, auto subscribers)
        {
            auto k = subscribers.getKey(samples.key);
            if(k)
            {
                if(_traceLevels->session > 2)
                {
                    Trace out(_traceLevels, _traceLevels->sessionCat);
                    out << _id << ": initiazing samples for key `" << k->get() << "'";
                }
                vector<shared_ptr<Sample>> samplesI;
                samplesI.reserve(samples.samples.size());
                for(auto& s : samples.samples)
                {
                    samplesI.push_back(topic->getSampleFactory()(s.id, s.type, k->get(), move(s.value), s.timestamp));
                }
                for(auto subscriber : k->getSubscribers())
                {
                    subscriber.first->initSamples(samplesI);
                }
            }
        });
    }
}

void
SubscriberSessionI::s(long long int id, long long int key, DataSample s, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(id, [&](auto topic, auto subscribers)
    {
        auto k = subscribers.getKey(key);
        if(k && subscribers.setLastId(s.id))
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": queuing sample `" << s.id << "' for key `" << k->get() << "'";
                if(!current.facet.empty())
                {
                    out << " (facet = " << current.facet << ')';
                }
            }

            auto impl = topic->getSampleFactory()(s.id, s.type, k->get(), move(s.value), s.timestamp);
            for(auto subscriber : k->getSubscribers())
            {
                if(current.facet == subscriber.second)
                {
                    subscriber.first->queue(impl);
                }
            }
        }
    });
}

void
SubscriberSessionI::f(long long int id, long long int filter, DataSample s, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(id, [&](auto topic, auto subscribers)
    {
        auto f = subscribers.getFilter(filter);
        if(f && subscribers.setLastId(s.id))
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": queuing sample `" << s.id << "' for filter `" << f->get() << "'";
                if(!current.facet.empty())
                {
                    out << " (facet = " << current.facet << ')';
                }
            }

            auto impl = topic->getSampleFactory()(s.id, s.type, nullptr, move(s.value), s.timestamp);
            if(f->get()->hasReaderMatch())
            {
                impl->decode(_instance->getCommunicator());
                if(!f->get()->readerMatch(impl))
                {
                    return;
                }
            }

            for(auto subscriber : f->getSubscribers())
            {
                if(current.facet == subscriber.second)
                {
                    subscriber.first->queue(impl);
                }
            }
        }
    });
}

bool
SubscriberSessionI::reconnect() const
{
    return _parent->createPublisherSession(_node);
}

PublisherSessionI::PublisherSessionI(NodeI* parent, const shared_ptr<NodePrx>& node) :
    SessionI(parent, node)
{
}

void
PublisherSessionI::destroy(const Ice::Current&)
{
    _parent->removePublisherSession(this);
}

vector<shared_ptr<TopicI>>
PublisherSessionI::getTopics(const string& name) const
{
    return _instance->getTopicFactory()->getTopicWriters(name);
}

bool
PublisherSessionI::reconnect() const
{
    return _parent->createSubscriberSession(_node);
}
