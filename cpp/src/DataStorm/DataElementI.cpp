// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/DataElementI.h>
#include <DataStorm/TopicI.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TraceUtil.h>
#include <DataStorm/CallbackExecutor.h>

using namespace std;
using namespace DataStormI;
using namespace DataStormContract;

namespace
{

DataSample
toSample(const shared_ptr<Sample>& sample, const shared_ptr<Ice::Communicator>& communicator, bool marshalKey)
{
    return { sample->id,
             marshalKey ? 0 : sample->key->getId(),
             marshalKey ? sample->key->encode(communicator) : Ice::ByteSeq {},
             chrono::time_point_cast<chrono::microseconds>(sample->timestamp).time_since_epoch().count(),
             sample->tag ? sample->tag->getId() : 0,
             sample->event,
             sample->encode(communicator) };
}

void
cleanOldSamples(deque<shared_ptr<Sample>>& samples,
                const chrono::time_point<chrono::system_clock>& now,
                int lifetime)
{
    chrono::time_point<chrono::system_clock> staleTime = now - chrono::milliseconds(lifetime);
    auto p = stable_partition(samples.begin(), samples.end(),
                              [&](const shared_ptr<Sample>& s) { return s->timestamp < staleTime; });
    if(p != samples.begin())
    {
        samples.erase(samples.begin(), p);
    }
}

}

DataElementI::DataElementI(TopicI* parent, const string& name, long long int id, const DataStorm::Config& config) :
    _traceLevels(parent->getInstance()->getTraceLevels()),
    _name(name),
    _id(id),
    _config(make_shared<ElementConfig>()),
    _executor(parent->getInstance()->getCallbackExecutor()),
    _listenerCount(0),
    _parent(parent->shared_from_this()),
    _waiters(0),
    _notified(0),
    _destroyed(false)
{
    _config->sampleCount = config.sampleCount;
    _config->sampleLifetime = config.sampleLifetime;
    if(!name.empty())
    {
        _config->name = name;
    }
    if(config.clearHistory)
    {
        _config->clearHistory = static_cast<ClearHistoryPolicy>(*config.clearHistory);
    }
}

void
DataElementI::init()
{
    _forwarder = Ice::uncheckedCast<SessionPrx>(_parent->getInstance()->getForwarderManager()->add(shared_from_this()));
}

DataElementI::~DataElementI()
{
    assert(_destroyed);
    assert(_listeners.empty());
    assert(_listenerCount == 0);
}

void
DataElementI::destroy()
{
    {
        unique_lock<mutex> lock(_parent->_mutex);
        assert(!_destroyed);
        _destroyed = true;
        destroyImpl(); // Must be called first.
    }
    disconnect();
    _parent->getInstance()->getForwarderManager()->remove(_forwarder->ice_getIdentity());
}

void
DataElementI::attach(long long int topicId,
                     long long int id,
                     const shared_ptr<Key>& key,
                     const shared_ptr<Filter>& filter,
                     const shared_ptr<SessionI>& session,
                     const shared_ptr<SessionPrx>& prx,
                     const ElementData& data,
                     const chrono::time_point<chrono::system_clock>& now,
                     ElementDataAckSeq& acks)
{
    shared_ptr<Filter> sampleFilter;
    if(data.config->sampleFilter)
    {
        auto info = *data.config->sampleFilter;
        sampleFilter = _parent->getSampleFilterFactories()->decode(getCommunicator(), info.name, info.criteria);
    }
    string facet = data.config->facet ? *data.config->facet : string();
    int priority = data.config->priority ? *data.config->priority : 0;
    string name;
    if(data.config->name)
    {
        name = *data.config->name;
    }
    else
    {
        ostringstream os;
        os << session->getId() << '-' << topicId << '-' << data.id;
        name = os.str();
    }
    if((id > 0 && attachKey(topicId, data.id, key, sampleFilter, session, prx, facet, id, name, priority)) ||
       (id < 0 && attachFilter(topicId, data.id, key, sampleFilter, session, prx, facet, id, filter, name, priority)))
    {
        auto q = data.lastIds.find(_id);
        long long lastId = q != data.lastIds.end() ? q->second : 0;
        LongLongDict lastIds = key ? session->getLastIds(topicId, id, shared_from_this()) : LongLongDict();
        DataSamples samples = getSamples(key, sampleFilter, data.config, lastId, now);
        acks.push_back({ _id, _config, lastIds, samples.samples, data.id });
    }
}

std::function<void()>
DataElementI::attach(long long int topicId,
                     long long int id,
                     const shared_ptr<Key>& key,
                     const shared_ptr<Filter>& filter,
                     const shared_ptr<SessionI>& session,
                     const shared_ptr<SessionPrx>& prx,
                     const ElementDataAck& data,
                     const chrono::time_point<chrono::system_clock>& now,
                     DataSamplesSeq& samples)
{
    shared_ptr<Filter> sampleFilter;
    if(data.config->sampleFilter)
    {
        auto info = *data.config->sampleFilter;
        sampleFilter = _parent->getSampleFilterFactories()->decode(getCommunicator(), info.name, info.criteria);
    }
    string facet = data.config->facet ? *data.config->facet : string();
    int priority = data.config->priority ? *data.config->priority : 0;
    string name;
    if(data.config->name)
    {
        name = *data.config->name;
    }
    else
    {
        ostringstream os;
        os << session->getId() << '-' << topicId << '-' << data.id;
        name = os.str();
    }
    if((id > 0 && attachKey(topicId, data.id, key, sampleFilter, session, prx, facet, id, name, priority)) ||
       (id < 0 && attachFilter(topicId, data.id, key, sampleFilter, session, prx, facet, id, filter, name, priority)))
    {
        auto q = data.lastIds.find(_id);
        long long lastId = q != data.lastIds.end() ? q->second : 0;
        samples.push_back(getSamples(key, sampleFilter, data.config, lastId, now));
    }

    auto samplesI = session->subscriberInitialized(topicId, id > 0 ? data.id : -data.id, data.samples, key,
                                                   shared_from_this());
    if(!samplesI.empty())
    {
        return [=]() { initSamples(samplesI, topicId, data.id, priority, now, id < 0); };
    }
    return nullptr;
}

bool
DataElementI::attachKey(long long int topicId,
                        long long int elementId,
                        const shared_ptr<Key>& key,
                        const shared_ptr<Filter>& sampleFilter,
                        const shared_ptr<SessionI>& session,
                        const shared_ptr<SessionPrx>& prx,
                        const string& facet,
                        long long int keyId,
                        const string& name,
                        int priority)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session, facet }, Listener(prx, facet)).first;
    }

    bool added = false;
    auto subscriber = p->second.addOrGet(topicId, elementId, keyId, nullptr, sampleFilter, name, priority, added);
    if(_onConnectedElements && added)
    {
        _executor->queue(shared_from_this(), [=]
        {
            _onConnectedElements(DataStorm::CallbackReason::Connect, name);
        });
    }
    if(addConnectedKey(key, subscriber))
    {
        if(key)
        {
            subscriber->keys.insert(key);
        }
        if(_traceLevels->data > 1)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << this << ": attach e" << elementId << ":" << name;
            if(!facet.empty())
            {
                out << ":" << facet;
            }
            out << ":[" << key << "]@" << topicId;
        }

        ++_listenerCount;
        _parent->incListenerCount(session);
        session->subscribeToKey(topicId, elementId, shared_from_this(), facet, key, keyId, name, priority);
        notifyListenerWaiters(session->getTopicLock());
        return true;
    }
    return false;
}

void
DataElementI::detachKey(long long int topicId,
                        long long int elementId,
                        const shared_ptr<Key>& key,
                        const shared_ptr<SessionI>& session,
                        const string& facet,
                        bool unsubscribe)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p == _listeners.end())
    {
        return;
    }

    auto subscriber = p->second.get(topicId, elementId);
    if(removeConnectedKey(key, subscriber))
    {
        if(key)
        {
            subscriber->keys.erase(key);
        }
        if(subscriber->keys.empty())
        {
            if(_onConnectedElements)
            {
                _executor->queue(shared_from_this(), [=]
                {
                    _onConnectedElements(DataStorm::CallbackReason::Disconnect, subscriber->name);
                });
            }
            if(p->second.remove(topicId, elementId))
            {
                _listeners.erase(p);
            }
        }

        if(_traceLevels->data > 1)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << this << ": detach e" << elementId << ":" << subscriber->name;
            if(!facet.empty())
            {
                out << ":" << facet;
            }
            out << ":[" << key << "]@" << topicId;
        }
        --_listenerCount;
        _parent->decListenerCount(session);
        if(unsubscribe)
        {
            session->unsubscribeFromKey(topicId, elementId, shared_from_this(), subscriber->id);
        }
        notifyListenerWaiters(session->getTopicLock());
    }
}

bool
DataElementI::attachFilter(long long int topicId,
                           long long int elementId,
                           const shared_ptr<Key>& key,
                           const shared_ptr<Filter>& sampleFilter,
                           const shared_ptr<SessionI>& session,
                           const shared_ptr<SessionPrx>& prx,
                           const string& facet,
                           long long int filterId,
                           const shared_ptr<Filter>& filter,
                           const string& name,
                           int priority)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session, facet }, Listener(prx, facet)).first;
    }

    bool added = false;
    auto subscriber = p->second.addOrGet(topicId, -elementId, filterId, filter, sampleFilter, name, priority, added);
    if(_onConnectedElements && added)
    {
        _executor->queue(shared_from_this(), [=]
        {
            _onConnectedElements(DataStorm::CallbackReason::Connect, name);
        });
    }
    if(addConnectedKey(key, subscriber))
    {
        if(key)
        {
            subscriber->keys.insert(key);
        }
        if(_traceLevels->data > 1)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << this << ": attach e" << elementId << ":" << name;
            if(!facet.empty())
            {
                out << ":" << facet;
            }
            out << ":[" << filter << "]@" << topicId;
        }

        ++_listenerCount;
        _parent->incListenerCount(session);
        session->subscribeToFilter(topicId, elementId, shared_from_this(), facet, key, name, priority);
        notifyListenerWaiters(session->getTopicLock());
        return true;
    }
    return false;
}

void
DataElementI::detachFilter(long long int topicId,
                           long long int elementId,
                           const shared_ptr<Key>& key,
                           const shared_ptr<SessionI>& session,
                           const string& facet,
                           bool unsubscribe)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p == _listeners.end())
    {
        return;
    }

    auto subscriber = p->second.get(topicId, -elementId);
    if(removeConnectedKey(key, subscriber))
    {
        if(key)
        {
            subscriber->keys.erase(key);
        }
        if(subscriber->keys.empty())
        {
            if(_onConnectedElements)
            {
                _executor->queue(shared_from_this(), [=]
                {
                    _onConnectedElements(DataStorm::CallbackReason::Disconnect, subscriber->name);
                });
            }
            if(p->second.remove(topicId, -elementId))
            {
                _listeners.erase(p);
            }
        }

        if(_traceLevels->data > 1)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << this << ": detach e" << elementId << ":" << subscriber->name;
            if(!facet.empty())
            {
                out << ":" << facet;
            }
            out << ":[" << subscriber->filter << "]@" << topicId;
        }

        --_listenerCount;
        _parent->decListenerCount(session);
        if(unsubscribe)
        {
            session->unsubscribeFromFilter(topicId, elementId, shared_from_this(), subscriber->id);
        }
        notifyListenerWaiters(session->getTopicLock());
    }
}

vector<shared_ptr<Key>>
DataElementI::getConnectedKeys() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    vector<shared_ptr<Key>> keys;
    for(const auto& key : _connectedKeys)
    {
        keys.push_back(key.first);
    }
    return keys;
}

vector<string>
DataElementI::getConnectedElements() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    vector<string> elements;
    elements.reserve(_listeners.size());
    for(const auto& listener : _listeners)
    {
        for(const auto& subscriber : listener.second.subscribers)
        {
            elements.push_back(subscriber.second->name);
        }
    }
    return elements;
}

void
DataElementI::onConnectedKeys(function<void(vector<shared_ptr<Key>>)> init,
                              function<void(DataStorm::CallbackReason, shared_ptr<Key>)> update)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onConnectedKeys = move(update);
    if(init)
    {
        vector<shared_ptr<Key>> keys;
        for(const auto& key : _connectedKeys)
        {
            keys.push_back(key.first);
        }
        _executor->queue(shared_from_this(), [init, keys] { init(keys); }, true);
    }
}

void
DataElementI::onConnectedElements(function<void(vector<string>)> init,
                                  function<void(DataStorm::CallbackReason, string)> update)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onConnectedElements = move(update);
    if(init)
    {
        vector<string> elements;
        for(const auto& listener : _listeners)
        {
            for(const auto& subscriber : listener.second.subscribers)
            {
                elements.push_back(subscriber.second->name);
            }
        }
        _executor->queue(shared_from_this(), [init, elements] { init(elements); }, true);
    }
}

void
DataElementI::initSamples(const vector<shared_ptr<Sample>>&,
                          long long int,
                          long long int,
                          int,
                          const chrono::time_point<chrono::system_clock>&,
                          bool)
{
}

DataSamples
DataElementI::getSamples(const shared_ptr<Key>&,
                         const shared_ptr<Filter>&,
                         const shared_ptr<DataStormContract::ElementConfig>&,
                         long long int,
                         const chrono::time_point<chrono::system_clock>&)
{
    return {};
}

void
DataElementI::queue(const shared_ptr<Sample>&, int, const shared_ptr<SessionI>&, const string&,
                    const chrono::time_point<chrono::system_clock>&, bool)
{
    assert(false);
}

shared_ptr<DataStormContract::ElementConfig>
DataElementI::getConfig() const
{
    return _config;
}

void
DataElementI::waitForListeners(int count) const
{
    unique_lock<mutex> lock(_parent->_mutex);
    ++_waiters;
    while(true)
    {
        _parent->getInstance()->checkShutdown();
        if(count < 0 && _listenerCount == 0)
        {
            --_waiters;
            return;
        }
        else if(count >= 0 && _listenerCount >= static_cast<size_t>(count))
        {
            --_waiters;
            return;
        }
        _parent->_cond.wait(lock);
        ++_notified;
    }
}

bool
DataElementI::hasListeners() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    return _listenerCount > 0;
}

shared_ptr<Ice::Communicator>
DataElementI::getCommunicator() const
{
    return _parent->getInstance()->getCommunicator();
}

bool
DataElementI::addConnectedKey(const shared_ptr<Key>& key, const shared_ptr<Subscriber>& subscriber)
{
    auto& subscribers = _connectedKeys[key];
    if(find(subscribers.begin(), subscribers.end(), subscriber) == subscribers.end())
    {
        if(key && subscribers.empty() && _onConnectedKeys)
        {
            _executor->queue(shared_from_this(), [=]
            {
                _onConnectedKeys(DataStorm::CallbackReason::Connect, key);
            });
        }
        subscribers.push_back(subscriber);
        return true;
    }
    return false;
}

bool
DataElementI::removeConnectedKey(const shared_ptr<Key>& key, const shared_ptr<Subscriber>& subscriber)
{
    auto& subscribers = _connectedKeys[key];
    auto p = find(subscribers.begin(), subscribers.end(), subscriber);
    if(p != subscribers.end())
    {
        subscribers.erase(p);
        if(subscribers.empty())
        {
            if(key && _onConnectedKeys)
            {
                _executor->queue(shared_from_this(), [=]
                {
                    _onConnectedKeys(DataStorm::CallbackReason::Disconnect, key);
                });
            }
            _connectedKeys.erase(key);
        }
        return true;
    }
    return false;
}

void
DataElementI::notifyListenerWaiters(unique_lock<mutex>& lock) const
{
    if(_waiters > 0)
    {
        _notified = 0;
        _parent->_cond.notify_all();
        _parent->_cond.wait(lock, [&]() { return _notified < _waiters; }); // Wait until all the waiters are notified.
    }
}

void
DataElementI::disconnect()
{
    map<ListenerKey, Listener> listeners;
    {
        unique_lock<mutex> lock(_parent->_mutex);
        listeners.swap(_listeners);
        _parent->decListenerCount(_listenerCount);
        _listenerCount = 0;
        notifyListenerWaiters(lock);
    }
    for(const auto& listener : listeners)
    {
        for(const auto& ks : listener.second.subscribers)
        {
            const auto& k = ks.first;
            if(k.second < 0)
            {
                listener.first.session->disconnectFromFilter(k.first, k.second, shared_from_this(), ks.second->id);
            }
            else
            {
                listener.first.session->disconnectFromKey(k.first, k.second, shared_from_this(), ks.second->id);
            }
        }
    }
}

void
DataElementI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    for(const auto& listener : _listeners)
    {
        // If there's at least one subscriber interested in the update
        if(!_sample || listener.second.matchOne(_sample, false))
        {
            listener.second.proxy->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
        }
    }
}

DataReaderI::DataReaderI(TopicReaderI* topic,
                         const string& name,
                         long long int id,
                         const string& sampleFilterName,
                         vector<unsigned char> sampleFilterCriteria,
                         const DataStorm::ReaderConfig& config) :
    DataElementI(topic, name, id, config),
    _parent(topic),
    _discardPolicy(config.discardPolicy ? *config.discardPolicy : DataStorm::DiscardPolicy::None)
{
    if(!sampleFilterName.empty())
    {
        _config->sampleFilter = FilterInfo { sampleFilterName, move(sampleFilterCriteria) };
    }
}

int
DataReaderI::getInstanceCount() const
{
    lock_guard<mutex> lock(_parent->_mutex);
    return _instanceCount;
}

vector<shared_ptr<Sample>>
DataReaderI::getAllUnread()
{
    lock_guard<mutex> lock(_parent->_mutex);
    vector<shared_ptr<Sample>> unread(_samples.begin(), _samples.end());
    _samples.clear();
    return unread;
}

void
DataReaderI::waitForUnread(unsigned int count) const
{
    unique_lock<mutex> lock(_parent->_mutex);
    _parent->_cond.wait(lock, [&]() { _parent->getInstance()->checkShutdown(); return _samples.size() >= count; });
}

bool
DataReaderI::hasUnread() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    return !_samples.empty();
}

shared_ptr<Sample>
DataReaderI::getNextUnread()
{
    unique_lock<mutex> lock(_parent->_mutex);
    _parent->_cond.wait(lock, [&]() { _parent->getInstance()->checkShutdown(); return !_samples.empty(); });
    shared_ptr<Sample> sample = _samples.front();
    _samples.pop_front();
    return sample;
}

void
DataReaderI::initSamples(const vector<shared_ptr<Sample>>& samples,
                         long long int topic,
                         long long int element,
                         int priority,
                         const chrono::time_point<chrono::system_clock>& now,
                         bool checkKey)
{
    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": initialized " << samples.size() << " samples from `" << element << '@' << topic << "'";
    }

    vector<shared_ptr<Sample>> valid;
    shared_ptr<Sample> previous = _last;
    for(const auto& sample : samples)
    {
        if(checkKey && !matchKey(sample->key))
        {
            continue;
        }
        else if(_discardPolicy == DataStorm::DiscardPolicy::SendTime &&
                sample->timestamp <= _lastSendTime)
        {
            continue;
        }
        else if(_discardPolicy == DataStorm::DiscardPolicy::Priority &&
                priority < _connectedKeys[sample->key].back()->priority)
        {
            continue;
        }
        assert(sample->key);
        valid.push_back(sample);

        if(!sample->hasValue())
        {
            if(sample->event == DataStorm::SampleEvent::PartialUpdate)
            {
                _parent->getUpdater(sample->tag)(previous, sample, _parent->getInstance()->getCommunicator());
            }
            else
            {
                sample->decode(_parent->getInstance()->getCommunicator());
            }
        }
        previous = sample;
    }

    if(_traceLevels->data > 2 && valid.size() < samples.size())
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": discarded " << samples.size() - valid.size() << " samples from `" << element << '@' << topic << "'";
    }

    if(_onSamples)
    {
        _executor->queue(shared_from_this(), [this, valid]
        {
            for(const auto& s : valid)
            {
                _onSamples(s);
            }
        });
    }

    if(valid.empty())
    {
        return;
    }
    _lastSendTime = valid.back()->timestamp;

    if(_config->sampleLifetime && *_config->sampleLifetime > 0)
    {
        cleanOldSamples(_samples, now, *_config->sampleLifetime);
    }

    if(_config->sampleCount)
    {
        if(*_config->sampleCount > 0)
        {
            size_t count = _samples.size();
            size_t maxCount = static_cast<size_t>(*_config->sampleCount);
            if(count + valid.size() > maxCount)
            {
                count = count + valid.size() - maxCount;
                while(!_samples.empty() && count-- > 0)
                {
                    _samples.pop_front();
                }
                assert(_samples.size() + valid.size() == maxCount);
            }
        }
        else if(*_config->sampleCount == 0)
        {
            return; // Don't keep history
        }
    }

    if(_config->clearHistory && *_config->clearHistory == ClearHistoryPolicy::OnAll)
    {
        _samples.clear();
        _samples.push_back(valid.back());
    }
    else
    {
        for(const auto& s : valid)
        {
            if(_config->clearHistory &&
               ((s->event == DataStorm::SampleEvent::Add && *_config->clearHistory == ClearHistoryPolicy::OnAdd) ||
                (s->event == DataStorm::SampleEvent::Remove && *_config->clearHistory == ClearHistoryPolicy::OnRemove) ||
                (s->event != DataStorm::SampleEvent::PartialUpdate &&
                 *_config->clearHistory == ClearHistoryPolicy::OnAllExceptPartialUpdate)))
            {
                _samples.clear();
            }
            _samples.push_back(s);
        }
    }
    assert(!_samples.empty());
    _last = _samples.back();
    _parent->_cond.notify_all();
}

void
DataReaderI::queue(const shared_ptr<Sample>& sample,
                   int priority,
                   const shared_ptr<SessionI>& session,
                   const string& facet,
                   const chrono::time_point<chrono::system_clock>& now,
                   bool checkKey)
{
    if(_config->facet && *_config->facet != facet)
    {
        if(_traceLevels->data > 2)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << this << ": skipped sample " << sample->id << " (facet doesn't match)";
        }
        return;
    }
    else if(checkKey && !matchKey(sample->key))
    {
        if(_traceLevels->data > 2)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << this << ": skipped sample " << sample->id << " (key doesn't match)";
        }
        return;
    }

    if(_traceLevels->data > 2)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": queued sample " << sample->id << " listeners=" << _listenerCount;
    }

    if((_discardPolicy == DataStorm::DiscardPolicy::SendTime && sample->timestamp <= _lastSendTime) ||
       (_discardPolicy == DataStorm::DiscardPolicy::Priority && priority < _connectedKeys[sample->key].back()->priority))
    {
        if(_traceLevels->data > 2)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << this << ": discarded sample" << sample->id;
        }
        return;
    }

    if(!sample->hasValue())
    {
        if(sample->event == DataStorm::SampleEvent::PartialUpdate)
        {
            _parent->getUpdater(sample->tag)(_last, sample, _parent->getInstance()->getCommunicator());
        }
        else
        {
            sample->decode(_parent->getInstance()->getCommunicator());
        }
    }
    _lastSendTime = sample->timestamp;

    if(_onSamples)
    {
        _executor->queue(shared_from_this(), [this, sample] { _onSamples(sample); });
    }

    if(_config->sampleLifetime && *_config->sampleLifetime > 0)
    {
        cleanOldSamples(_samples, now, *_config->sampleLifetime);
    }

    if(_config->sampleCount)
    {
        if(*_config->sampleCount > 0)
        {
            size_t count = _samples.size();
            size_t maxCount = static_cast<size_t>(*_config->sampleCount);
            if(count + 1 > maxCount)
            {
                if(!_samples.empty())
                {
                    _samples.pop_front();
                }
                assert(_samples.size() + 1 == maxCount);
            }
        }
        else if(*_config->sampleCount == 0)
        {
            return; // Don't keep history
        }
    }

    if(_config->clearHistory &&
       (*_config->clearHistory == ClearHistoryPolicy::OnAll ||
        (sample->event == DataStorm::SampleEvent::Add && *_config->clearHistory == ClearHistoryPolicy::OnAdd) ||
        (sample->event == DataStorm::SampleEvent::Remove && *_config->clearHistory == ClearHistoryPolicy::OnRemove) ||
        (sample->event != DataStorm::SampleEvent::PartialUpdate &&
         *_config->clearHistory == ClearHistoryPolicy::OnAllExceptPartialUpdate)))
    {
        _samples.clear();
    }
    _samples.push_back(sample);
    _last = sample;
    _parent->_cond.notify_all();
}

void
DataReaderI::onSamples(function<void(const vector<shared_ptr<Sample>>&)> init,
                       function<void(const shared_ptr<Sample>&)> update)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onSamples = move(update);
    if(init && !_samples.empty())
    {
        vector<shared_ptr<Sample>> samples(_samples.begin(), _samples.end());
        _executor->queue(shared_from_this(), [init, samples] { init(samples); }, true);
    }
}

bool
DataReaderI::addConnectedKey(const shared_ptr<Key>& key, const shared_ptr<Subscriber>& subscriber)
{
    if(DataElementI::addConnectedKey(key, subscriber))
    {
        if(_discardPolicy == DataStorm::DiscardPolicy::Priority)
        {
            auto& subscribers = _connectedKeys[key];
            sort(subscribers.begin(), subscribers.end(), [](const shared_ptr<Subscriber>& lhs,
                                                            const shared_ptr<Subscriber>& rhs)
            {
                return lhs->priority < rhs->priority;
            });
        }
        return true;
    }
    else
    {
        return false;
    }
}

DataWriterI::DataWriterI(TopicWriterI* topic,
                         const string& name,
                         long long int id,
                         const DataStorm::WriterConfig& config) :
    DataElementI(topic, name, id, config),
    _parent(topic)
{
    _config->priority = config.priority;
}

void
DataWriterI::init()
{
    DataElementI::init();
    _subscribers = Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(_forwarder);
}

void
DataWriterI::publish(const shared_ptr<Key>& key, const shared_ptr<Sample>& sample)
{
    lock_guard<mutex> lock(_parent->_mutex);
    if(sample->event == DataStorm::SampleEvent::PartialUpdate)
    {
        assert(!sample->hasValue());
        _parent->getUpdater(sample->tag)(_last, sample, _parent->getInstance()->getCommunicator());
    }

    sample->id = ++_parent->_nextSampleId;
    sample->timestamp = chrono::system_clock::now();

    if(_traceLevels->data > 2)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": publishing sample " << sample->id << " listeners=" << _listenerCount;
    }
    send(key, sample);

    if(_config->sampleLifetime && *_config->sampleLifetime > 0)
    {
        cleanOldSamples(_samples, sample->timestamp, *_config->sampleLifetime);
    }

    if(_config->sampleCount)
    {
        if(*_config->sampleCount > 0)
        {
            if(_samples.size() + 1 > static_cast<size_t>(*_config->sampleCount))
            {
                _samples.pop_front();
            }
        }
        else if(*_config->sampleCount == 0)
        {
            return; // Don't keep history
        }
    }

    if(_config->clearHistory &&
       (*_config->clearHistory == ClearHistoryPolicy::OnAll ||
        (sample->event == DataStorm::SampleEvent::Add && *_config->clearHistory == ClearHistoryPolicy::OnAdd) ||
        (sample->event == DataStorm::SampleEvent::Remove && *_config->clearHistory == ClearHistoryPolicy::OnRemove) ||
        (sample->event != DataStorm::SampleEvent::PartialUpdate &&
         *_config->clearHistory == ClearHistoryPolicy::OnAllExceptPartialUpdate)))
    {
        _samples.clear();
    }
    assert(sample->key);
    _samples.push_back(sample);
    _last = sample;
}

KeyDataReaderI::KeyDataReaderI(TopicReaderI* topic,
                               const string& name,
                               long long int id,
                               const vector<shared_ptr<Key>>& keys,
                               const string& sampleFilterName,
                               const vector<unsigned char> sampleFilterCriteria,
                               const DataStorm::ReaderConfig& config) :
    DataReaderI(topic, name, id, sampleFilterName, sampleFilterCriteria, config),
    _keys(keys)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": created key reader";
    }

    //
    // If sample filtering is enabled, ensure the updates are received using a session
    // facet specific to this reader.
    //
    if(_config->sampleFilter)
    {
        ostringstream os;
        os << "fa" << _id;
        _config->facet = os.str();
    }
}

void
KeyDataReaderI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": destroyed key reader";
    }
    try
    {
        _forwarder->detachElements(_parent->getId(), { _keys.empty() ? -_id : _id });
    }
    catch(const std::exception&)
    {
        _parent->forwarderException();
    }
    _parent->remove(shared_from_this(), _keys);
}

void
KeyDataReaderI::waitForWriters(int count)
{
    waitForListeners(count);
}

bool
KeyDataReaderI::hasWriters()
{
    return hasListeners();
}

string
KeyDataReaderI::toString() const
{
    ostringstream os;
    os << 'e' << _id << ":";
    if(_config->name)
    {
        os << *_config->name << ":";
    }
    os << "[";
    for(auto q = _keys.begin(); q != _keys.end(); ++q)
    {
        if(q != _keys.begin())
        {
            os << ",";
        }
        os << (*q)->toString();
    }
    os << "]@" << _parent;
    return os.str();
}

bool
KeyDataReaderI::matchKey(const shared_ptr<Key>& key) const
{
    return _keys.empty() || find(_keys.begin(), _keys.end(), key) != _keys.end();
}

KeyDataWriterI::KeyDataWriterI(TopicWriterI* topic,
                               const string& name,
                               long long int id,
                               const vector<shared_ptr<Key>>& keys,
                               const DataStorm::WriterConfig& config) :
    DataWriterI(topic, name, id, config),
    _keys(keys)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": created key writer";
    }
}

void
KeyDataWriterI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": destroyed key writer";
    }
    try
    {
        _forwarder->detachElements(_parent->getId(), { _keys.empty() ? -_id : _id });
    }
    catch(const std::exception&)
    {
        _parent->forwarderException();
    }
    _parent->remove(shared_from_this(), _keys);
}

void
KeyDataWriterI::waitForReaders(int count) const
{
    waitForListeners(count);
}

bool
KeyDataWriterI::hasReaders() const
{
    return hasListeners();
}

shared_ptr<Sample>
KeyDataWriterI::getLast() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    return _samples.empty () ? nullptr : _samples.back();
}

vector<shared_ptr<Sample>>
KeyDataWriterI::getAll() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    vector<shared_ptr<Sample>> all(_samples.begin(), _samples.end());
    return all;
}

string
KeyDataWriterI::toString() const
{
    ostringstream os;
    os << 'e' << _id << ":";
    if(_config->name)
    {
        os << *_config->name << ":";
    }
    os << "[";
    for(auto q = _keys.begin(); q != _keys.end(); ++q)
    {
        if(q != _keys.begin())
        {
            os << ",";
        }
        os << (*q)->toString();
    }
    os << "]@" << _parent;
    return os.str();
}

DataSamples
KeyDataWriterI::getSamples(const shared_ptr<Key>& key,
                           const shared_ptr<Filter>& sampleFilter,
                           const shared_ptr<DataStormContract::ElementConfig>& config,
                           long long int lastId,
                           const chrono::time_point<chrono::system_clock>& now)
{
    DataSamples samples;
    samples.id = _keys.empty() ? -_id : _id;

    if(config->sampleCount && *config->sampleCount == 0)
    {
        return samples;
    }

    if(_config->sampleLifetime && *_config->sampleLifetime > 0)
    {
        cleanOldSamples(_samples, now, *_config->sampleLifetime);
    }

    chrono::time_point<chrono::system_clock> staleTime = chrono::time_point<chrono::system_clock>::min();
    if(config->sampleLifetime && *config->sampleLifetime > 0)
    {
        staleTime = now - chrono::milliseconds(*config->sampleLifetime);
    }

    shared_ptr<Sample> first;
    for(auto p = _samples.rbegin(); p != _samples.rend(); ++p)
    {
        if((*p)->timestamp < staleTime)
        {
            break;
        }
        if((*p)->id <= lastId)
        {
            break;
        }

        if((!key || key == (*p)->key) && (!sampleFilter || sampleFilter->match(*p)))
        {
            first = *p;
            samples.samples.push_front(toSample(*p, getCommunicator(), _keys.empty()));
            if(config->sampleCount &&
               *config->sampleCount > 0 && static_cast<size_t>(*config->sampleCount) == samples.samples.size())
            {
                break;
            }

            if(config->clearHistory &&
               (*config->clearHistory == ClearHistoryPolicy::OnAll ||
                ((*p)->event == DataStorm::SampleEvent::Add && *config->clearHistory == ClearHistoryPolicy::OnAdd) ||
                ((*p)->event == DataStorm::SampleEvent::Remove && *config->clearHistory == ClearHistoryPolicy::OnRemove) ||
                ((*p)->event != DataStorm::SampleEvent::PartialUpdate &&
                     *config->clearHistory == ClearHistoryPolicy::OnAllExceptPartialUpdate)))
            {
                break;
            }
        }
    }
    if(!samples.samples.empty())
    {
        // If the first sample is a partial update, transform it to a full Update
        if(first->event == DataStorm::SampleEvent::PartialUpdate)
        {
            samples.samples[0] = {
                first->id,
                samples.samples[0].keyId,
                samples.samples[0].keyValue,
                chrono::time_point_cast<chrono::microseconds>(first->timestamp).time_since_epoch().count(),
                0,
                DataStorm::SampleEvent::Update,
                first->encodeValue(getCommunicator()) };
        }
    }
    return samples;
}

void
KeyDataWriterI::send(const shared_ptr<Key>& key, const shared_ptr<Sample>& sample) const
{
    assert(key || _keys.size() == 1);
    _sample = sample;
    _sample->key = key ? key : _keys[0];
    _subscribers->s(_parent->getId(), _keys.empty() ? -_id : _id, toSample(sample, getCommunicator(), _keys.empty()));
    _sample = nullptr;
}

void
KeyDataWriterI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    for(const auto& listener : _listeners)
    {
        // If there's at least one subscriber interested in the update (check the key if any writer)
        if(!_sample || listener.second.matchOne(_sample, _keys.empty()))
        {
            listener.second.proxy->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
        }
    }
}

FilteredDataReaderI::FilteredDataReaderI(TopicReaderI* topic,
                                         const string& name,
                                         long long int id,
                                         const shared_ptr<Filter>& filter,
                                         const string& sampleFilterName,
                                         vector<unsigned char> sampleFilterCriteria,
                                         const DataStorm::ReaderConfig& config) :
    DataReaderI(topic, name, id, sampleFilterName, sampleFilterCriteria, config),
    _filter(filter)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": created filtered reader";
    }

    //
    // If sample filtering is enabled, ensure the updates are received using a session
    // facet specific to this reader.
    //
    if(_config->sampleFilter)
    {
        ostringstream os;
        os << "fa" << _id;
        _config->facet = os.str();
    }
}

void
FilteredDataReaderI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": destroyed filter reader";
    }
    try
    {
        _forwarder->detachElements(_parent->getId(), { -_id });
    }
    catch(const std::exception&)
    {
        _parent->forwarderException();
    }
    _parent->removeFiltered(shared_from_this(), _filter);
}

void
FilteredDataReaderI::waitForWriters(int count)
{
     waitForListeners(count);
}

bool
FilteredDataReaderI::hasWriters()
{
     return hasListeners();
}

string
FilteredDataReaderI::toString() const
{
    ostringstream os;
    os << 'e' << _id << ':';
    if(_config->name)
    {
        os << *_config->name << ":";
    }
    os << "[" << _filter->toString() << "]@" << _parent;
    return os.str();
}

bool
FilteredDataReaderI::matchKey(const shared_ptr<Key>& key) const
{
    return _filter->match(key);
}
