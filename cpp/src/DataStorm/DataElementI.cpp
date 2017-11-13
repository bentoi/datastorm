// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/DataElementI.h>
#include <DataStorm/TopicI.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TraceUtil.h>

using namespace std;
using namespace DataStormInternal;
using namespace DataStormContract;

namespace
{

DataSample
toSample(const shared_ptr<Sample>& sample, const shared_ptr<Ice::Communicator>& communicator)
{
    return { sample->id,
             chrono::time_point_cast<chrono::microseconds>(sample->timestamp).time_since_epoch().count(),
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

DataElementI::DataElementI(TopicI* parent, long long int id, const DataStorm::Config& config) :
    _traceLevels(parent->getInstance()->getTraceLevels()),
    _forwarder(Ice::uncheckedCast<SessionPrx>(parent->getInstance()->getForwarderManager()->add(this))),
    _id(id),
    _listenerCount(0),
    _config(make_shared<ElementConfig>()),
    _parent(parent),
    _waiters(0),
    _notified(0)
{
    if(config.sampleCount)
    {
        _config->sampleCount = *config.sampleCount;
    }
    if(config.sampleLifetime)
    {
        _config->sampleLifetime = *config.sampleLifetime;
    }
}

DataElementI::~DataElementI()
{
    disconnect();
    _parent->getInstance()->getForwarderManager()->remove(_forwarder->ice_getIdentity());
}

void
DataElementI::destroy()
{
    {
        unique_lock<mutex> lock(_parent->_mutex);
        destroyImpl(); // Must be called first.
    }
    disconnect();
}

void
DataElementI::attach(long long int topicId,
                     const shared_ptr<Key>& key,
                     const shared_ptr<Filter>& filter,
                     SessionI* session,
                     const shared_ptr<SessionPrx>& prx,
                     const ElementData& data,
                     long long int lastId,
                     const chrono::time_point<chrono::system_clock>& now,
                     ElementDataAckSeq& acks)
{
    auto sampleFilter = data.config->sampleFilter ? createSampleFilter(*data.config->sampleFilter) : nullptr;
    string facet = data.config->facet ? *data.config->facet : string();
    if((key && attachKey(topicId, data.id, key, sampleFilter, session, prx, facet)) ||
       (filter && attachFilter(topicId, data.id, filter, sampleFilter, session, prx, facet)))
    {
        acks.push_back({ _id, _config, getSamples(lastId, key, sampleFilter, data.config, now), data.id });
    }
}

void
DataElementI::attach(long long int topicId,
                     const shared_ptr<Key>& key,
                     const shared_ptr<Filter>& filter,
                     SessionI* session,
                     const shared_ptr<SessionPrx>& prx,
                     const ElementDataAck& data,
                     long long int lastId,
                     const chrono::time_point<chrono::system_clock>& now,
                     DataSamplesSeq& samples)
{
    auto sampleFilter = data.config->sampleFilter ? createSampleFilter(*data.config->sampleFilter) : nullptr;
    string facet = data.config->facet ? *data.config->facet : string();
    if((key && attachKey(topicId, data.id, key, sampleFilter, session, prx, facet)) ||
       (filter && attachFilter(topicId, data.id, filter, sampleFilter, session, prx, facet)))
    {
        samples.push_back({ _id, getSamples(lastId, key, sampleFilter, data.config, now) });
    }

    if(_parent->_sampleFactory && !data.samples.empty())
    {
        vector<shared_ptr<Sample>> samplesI;
        samplesI.reserve(data.samples.size());
        for(auto& s : data.samples)
        {
            samplesI.push_back(_parent->_sampleFactory->create(session->getId(),
                                                               topicId,
                                                               data.id,
                                                               s.id,
                                                               s.event,
                                                               key,
                                                               s.value,
                                                               s.timestamp));
        }
        initSamples(samplesI, topicId, data.id, now);
    }
    session->subscriberInitialized(topicId, data.id, this);
}

bool
DataElementI::attachKey(long long int topicId,
                        long long int elementId,
                        const shared_ptr<Key>& key,
                        const shared_ptr<Filter>& sampleFilter,
                        SessionI* session,
                        const shared_ptr<SessionPrx>& prx,
                        const string& facet)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session, facet }, Listener(prx, facet)).first;
    }
    if(p->second.keys.add(topicId, elementId, sampleFilter))
    {
        ++_listenerCount;
        session->subscribeToKey(topicId, elementId, key, this, facet);
        notifyListenerWaiters(session->getTopicLock());
        if(_onConnect)
        {
            _parent->queue(shared_from_this(), [=] { _onConnect(make_tuple(session->getId(), topicId, elementId)); });
        }
        return true;
    }
    else
    {
        return false;
    }
}

void
DataElementI::detachKey(long long int topicId,
                        long long int elementId,
                        SessionI* session,
                        const string& facet,
                        bool unsubscribe)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p != _listeners.end() && p->second.keys.remove(topicId, elementId))
    {
        --_listenerCount;
        if(unsubscribe)
        {
            session->unsubscribeFromKey(topicId, elementId, this);
        }
        if(_onDisconnect)
        {
            _parent->queue(shared_from_this(), [=] { _onDisconnect(make_tuple(session->getId(), topicId, elementId)); });
        }
        notifyListenerWaiters(session->getTopicLock());
        if(p->second.empty())
        {
            _listeners.erase(p);
        }
    }
}

bool
DataElementI::attachFilter(long long int topicId,
                           long long int elementId,
                           const shared_ptr<Filter>& filter,
                           const shared_ptr<Filter>& sampleFilter,
                           SessionI* session,
                           const shared_ptr<SessionPrx>& prx,
                           const string& facet)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session, facet }, Listener(prx, facet)).first;
    }
    if(p->second.filters.add(topicId, elementId, sampleFilter))
    {
        ++_listenerCount;
        session->subscribeToFilter(topicId, elementId, filter, this, facet);
        if(_onConnect)
        {
            _parent->queue(shared_from_this(), [=] { _onConnect(make_tuple(session->getId(), topicId, elementId)); });
        }
        notifyListenerWaiters(session->getTopicLock());
        return true;
    }
    else
    {
        return false;
    }
}

void
DataElementI::detachFilter(long long int topicId,
                           long long int elementId,
                           SessionI* session,
                           const string& facet,
                           bool unsubscribe)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p != _listeners.end() && p->second.filters.remove(topicId, elementId))
    {
        --_listenerCount;
        if(unsubscribe)
        {
            session->unsubscribeFromFilter(topicId, elementId, this);
        }
        if(_onDisconnect)
        {
            _parent->queue(shared_from_this(), [=] { _onDisconnect(make_tuple(session->getId(), topicId, elementId)); });
        }
        notifyListenerWaiters(session->getTopicLock());
        if(p->second.empty())
        {
            _listeners.erase(p);
        }
    }
}

void
DataElementI::onConnect(function<void(tuple<string, long long int, long long int>)> callback)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onConnect = move(callback);
}

void
DataElementI::onDisconnect(function<void(tuple<string, long long int, long long int>)> callback)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onDisconnect = move(callback);
}

void
DataElementI::initSamples(const vector<shared_ptr<Sample>>&,
                          long long int,
                          long long int,
                          const chrono::time_point<chrono::system_clock>&)
{
}

DataSampleSeq
DataElementI::getSamples(long long int,
                         const shared_ptr<Key>&,
                         const shared_ptr<Filter>&,
                         const shared_ptr<DataStormContract::ElementConfig>&,
                         const chrono::time_point<chrono::system_clock>&)
{
    return {};
}

void
DataElementI::queue(const shared_ptr<Sample>&, const string&, const chrono::time_point<chrono::system_clock>&)
{
    assert(false);
}

shared_ptr<Filter>
DataElementI::createSampleFilter(vector<unsigned char>) const
{
    return nullptr;
}

void
DataElementI::waitForListeners(int count) const
{
    unique_lock<mutex> lock(_parent->_mutex);
    ++_waiters;
    while(true)
    {
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
        notifyListenerWaiters(lock);
        _listenerCount = 0;
    }
    for(const auto& listener : listeners)
    {
        for(const auto& ks : listener.second.keys.subscribers)
        {
            listener.first.session->disconnectFromKey(ks.first.first, ks.first.second, this);
        }
        for(const auto& fs : listener.second.filters.subscribers)
        {
            listener.first.session->disconnectFromFilter(fs.first.first, fs.first.second, this);
        }
    }
}

void
DataElementI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    for(const auto& listener : _listeners)
    {
        if(!_sample || listener.second.matchOne(_sample)) // If there's at least one subscriber interested in the update
        {
            listener.second.proxy->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
        }
    }
}

DataReaderI::DataReaderI(TopicReaderI* topic, long long int id, vector<unsigned char> sampleFilterCriteria,
                         const DataStorm::ReaderConfig& config) :
    DataElementI(topic, id, config),
    _parent(topic)
{
    if(!sampleFilterCriteria.empty())
    {
        _config->sampleFilter = move(sampleFilterCriteria);
    }
}

int
DataReaderI::getInstanceCount() const
{
    lock_guard<mutex> lock(_parent->_mutex);
    return _instanceCount;
}

vector<shared_ptr<Sample>>
DataReaderI::getAll()
{
    getAllUnread(); // Read and add unread

    lock_guard<mutex> lock(_parent->_mutex);
    return vector<shared_ptr<Sample>>(_all.begin(), _all.end());
}

vector<shared_ptr<Sample>>
DataReaderI::getAllUnread()
{
    lock_guard<mutex> lock(_parent->_mutex);
    vector<shared_ptr<Sample>> unread(_unread.begin(), _unread.end());
    for(const auto& s : unread)
    {
        _all.push_back(s);
    }
    _unread.clear();
    return unread;
}

void
DataReaderI::waitForUnread(unsigned int count) const
{
    unique_lock<mutex> lock(_parent->_mutex);
    while(_unread.size() < count)
    {
        _parent->_cond.wait(lock);
    }
}

bool
DataReaderI::hasUnread() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    return !_unread.empty();
}

shared_ptr<Sample>
DataReaderI::getNextUnread()
{
    unique_lock<mutex> lock(_parent->_mutex);
    _parent->_cond.wait(lock, [&]() { return !_unread.empty(); });
    shared_ptr<Sample> sample = _unread.front();
    _unread.pop_front();
    _all.push_back(sample);
    return sample;
}

void
DataReaderI::initSamples(const vector<shared_ptr<Sample>>& samples,
                         long long int topic,
                         long long int element,
                         const chrono::time_point<chrono::system_clock>& now)
{
    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": initialized " << samples.size() << " samples from `" << element << '@' << topic << "'";
    }

    if(_onInit)
    {
        _parent->queue(shared_from_this(), [this, samples] { _onInit(samples); });
    }

    if(_config->sampleLifetime)
    {
        cleanOldSamples(_all, now, *_config->sampleLifetime);
        cleanOldSamples(_unread, now, *_config->sampleLifetime);
    }

    bool trimOnAdd = false;
    if(_config->sampleCount)
    {
        if(*_config->sampleCount > 0)
        {
            size_t count = _all.size() + _unread.size();
            size_t maxCount = static_cast<size_t>(*_config->sampleCount);
            if(count + samples.size() > maxCount)
            {
                count = count + samples.size() - maxCount;
                while(!_all.empty() && count-- > 0)
                {
                    _all.pop_front();
                }
                while(!_unread.empty() && count-- > 0)
                {
                    _unread.pop_front();
                }
                assert(_all.size() + _unread.size() + samples.size() == maxCount);
            }
        }
        else if(*_config->sampleCount < 0)
        {
            trimOnAdd = true;
        }
        else if(*_config->sampleCount == 0)
        {
            return; // Don't keep history
        }
    }

    for(const auto& s : samples)
    {
        if(trimOnAdd && s->event == DataStorm::SampleEvent::Add)
        {
            _all.clear();
            _unread.clear();
        }
        _unread.push_back(s);
    }
    _parent->_cond.notify_all();
}

void
DataReaderI::queue(const shared_ptr<Sample>& sample,
                   const string& facet,
                   const chrono::time_point<chrono::system_clock>& now)
{
    if(_config->facet && *_config->facet != facet)
    {
        return;
    }

    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": queued sample " << sample->id << " listeners=" << _listenerCount;
    }

    if(_onSample)
    {
        _parent->queue(shared_from_this(), [this, sample] { _onSample(sample); });
    }

    if(_config->sampleLifetime)
    {
        cleanOldSamples(_all, now, *_config->sampleLifetime);
        cleanOldSamples(_unread, now, *_config->sampleLifetime);
    }

    if(_config->sampleCount)
    {
        if(*_config->sampleCount > 0)
        {
            size_t count = _all.size() + _unread.size();
            size_t maxCount = static_cast<size_t>(*_config->sampleCount);
            if(count + 1 > maxCount)
            {
                if(!_all.empty())
                {
                    _all.pop_front();
                }
                else if(!_unread.empty())
                {
                    _unread.pop_front();
                }
                assert(_all.size() + _unread.size() + 1 == maxCount);
            }
        }
        else if(*_config->sampleCount < 0 && sample->event == DataStorm::SampleEvent::Add)
        {
            _all.clear();
            _unread.clear();
        }
        else if(*_config->sampleCount == 0)
        {
            return; // Don't keep history
        }
    }

    _unread.push_back(sample);
    _parent->_cond.notify_all();
}

void
DataReaderI::onInit(function<void(const vector<shared_ptr<Sample>>&)> callback)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onInit = move(callback);
}

void
DataReaderI::onSample(function<void(const shared_ptr<Sample>&)> callback)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onSample = move(callback);
}

DataWriterI::DataWriterI(TopicWriterI* topic, long long int id,
                         const shared_ptr<FilterFactory>& sampleFilterFactory,
                         const DataStorm::WriterConfig& config) :
    DataElementI(topic, id, config),
    _parent(topic),
    _sampleFilterFactory(sampleFilterFactory),
    _subscribers(Ice::uncheckedCast<SubscriberSessionPrx>(_forwarder))
{
}

void
DataWriterI::publish(const shared_ptr<Key>& key, const shared_ptr<Sample>& sample)
{
    lock_guard<mutex> lock(_parent->_mutex);

    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": publishing sample " << sample->id << " listeners=" << _listenerCount;
    }
    sample->id = ++_parent->_nextSampleId;
    sample->timestamp = chrono::system_clock::now();
    send(key, sample);

    if(_config->sampleLifetime)
    {
        cleanOldSamples(_all, sample->timestamp, *_config->sampleLifetime);
    }

    if(_config->sampleCount)
    {
        if(*_config->sampleCount > 0)
        {
            if(_all.size() + 1 > static_cast<size_t>(*_config->sampleCount))
            {
                _all.pop_front();
            }
        }
        else if(*_config->sampleCount < 0 && sample->event == DataStorm::SampleEvent::Add)
        {
            _all.clear();
        }
        else if(*_config->sampleCount == 0)
        {
            return; // Don't keep history
        }
    }
    _all.push_back(sample);
}

shared_ptr<Filter>
DataWriterI::createSampleFilter(vector<unsigned char> sampleFilter) const
{
    if(_sampleFilterFactory && !sampleFilter.empty())
    {
        return _sampleFilterFactory->decode(_parent->getInstance()->getCommunicator(), move(sampleFilter));
    }
    return nullptr;
}

KeyDataReaderI::KeyDataReaderI(TopicReaderI* topic,
                               long long int id,
                               const vector<shared_ptr<Key>>& keys,
                               const vector<unsigned char> sampleFilterCriteria,
                               const DataStorm::ReaderConfig& config) :
    DataReaderI(topic, id, sampleFilterCriteria, config),
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
    _forwarder->detachElements(_parent->getId(), { _id });
    _parent->remove(_keys, shared_from_this());
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
    os << 'e' << _id << ":[";
    for(auto q = _keys.begin(); q != _keys.end(); ++q)
    {
        if(q != _keys.begin())
        {
            os << ",";
        }
        os << (*q)->toString();
    }
    os << "]@" << _parent->getId();
    return os.str();
}

KeyDataWriterI::KeyDataWriterI(TopicWriterI* topic,
                               long long int id,
                               const vector<shared_ptr<Key>>& keys,
                               const shared_ptr<FilterFactory>& sampleFilterFactory,
                               const DataStorm::WriterConfig& config) :
    DataWriterI(topic, id, sampleFilterFactory, config),
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
    _forwarder->detachElements(_parent->getId(), { _id });
    _parent->remove(_keys, shared_from_this());
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

string
KeyDataWriterI::toString() const
{
    ostringstream os;
    os << 'e' << _id << ":[";
    for(auto q = _keys.begin(); q != _keys.end(); ++q)
    {
        if(q != _keys.begin())
        {
            os << ",";
        }
        os << (*q)->toString();
    }
    os << "]@" << _parent->getId();
    return os.str();
}

DataSampleSeq
KeyDataWriterI::getSamples(long long int lastId,
                           const shared_ptr<Key>& key,
                           const shared_ptr<Filter>& filter,
                           const shared_ptr<DataStormContract::ElementConfig>& config,
                           const chrono::time_point<chrono::system_clock>& now)
{
    DataSampleSeq samples;
    if(config->sampleCount && *config->sampleCount == 0)
    {
        return samples;
    }

    if(_config->sampleLifetime)
    {
        cleanOldSamples(_all, now, *_config->sampleLifetime);
    }

    chrono::time_point<chrono::system_clock> staleTime;
    if(config->sampleLifetime)
    {
        staleTime = now - chrono::milliseconds(*config->sampleLifetime);
    }

    for(auto p = _all.rbegin(); p != _all.rend(); ++p)
    {
        if((*p)->id <= lastId || (config->sampleLifetime && (*p)->timestamp < staleTime))
        {
            break;
        }
        if((!key || key == (*p)->key) && (!filter || filter->match(*p)))
        {
            samples.push_front(toSample(*p, nullptr)); // The sample should already be encoded
            if(config->sampleCount &&
               ((*config->sampleCount < 0 && (*p)->event == DataStorm::SampleEvent::Add) ||
                static_cast<size_t>(*config->sampleCount) == samples.size()))
            {
                break;
            }
        }
    }
    return samples;
}

void
KeyDataWriterI::send(const shared_ptr<Key>& key, const shared_ptr<Sample>& sample) const
{
    _sample = sample;
    _sample->key = key ? key : _keys[0];
    _subscribers->s(_parent->getId(), _id, toSample(sample, getCommunicator()));
    _sample = nullptr;
}

FilteredDataReaderI::FilteredDataReaderI(TopicReaderI* topic,
                                         long long int id,
                                         const shared_ptr<Filter>& filter,
                                         vector<unsigned char> sampleFilterCriteria,
                                         const DataStorm::ReaderConfig& config) :
    DataReaderI(topic, id, sampleFilterCriteria, config),
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
    _forwarder->detachElements(_parent->getId(), { -_id });
    _parent->removeFiltered(_filter, shared_from_this());
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
    os << 'e' << _id << ":" << _filter->toString() << "@" << _parent->getId();
    return os.str();
}
