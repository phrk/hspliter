#include <set>

#include "hSpliterLocal.h"

/*
hSpliterLocal::KeyState::KeyState(bool _owned, bool _handled):
//			owned(_owned),
			handled(_handled)
{
}

hSpliterLocal::KeyState::KeyState(std::string _owned, std::string _handled)
{
//	owned = 0;
	handled = 0;
	if (_owned=="1")
	{
//		owned = 1;
	}
	else if (_owned=="0")
	{
	}
	else
	{
//		throw "KeyState::KeyState _owned undefined value: " + _owned;
	}
	
	if (_handled=="1")
	{
		handled = 1;
	}
	else if (_handled=="0")
	{
		handled = 0;
	}
	else
	{
//		throw "KeyState::KeyState handled undefined value: " + _handled;
	}
}*/	

bool stringToBool(const std::string &_handled)
{
	if (_handled=="1") {
		return true;
	}
	else if (_handled=="0") {
		return false;
	}
	else {
		throw "stringToBool undefined value: " + _handled;
	}
}
		
hSpliterLocal::hSpliterLocal(htConnPoolPtr conn_pool,
				std::string _ns,
				std::string _input_table,
				std::string _job,
				hSpliterClient::Mode mode,
				size_t key_step)
{
	m_conn_pool.reset(new htConnPool(*conn_pool));
	m_job = _job;
	m_key_step = key_step;
	m_lock.reset(new hAutoLock);
	m_nhandled = 0;
	
	if (mode == hSpliterClient::START) {
		{
			htConnPool::htSession sess = m_conn_pool->get();
			m_ns = sess.client->namespace_open(_ns);
			Hypertable::ThriftGen::HqlResult result;
			sess.client->hql_query(result, m_ns, "drop table if exists "+_job);
			sess.client->hql_query(result, m_ns, "create table "+_job+\
				" (handled MAX_VERSIONS=1)");
			std::cout << "SPLITER TABLES DROPPED \n";
			sleep(2);
		}
		createDbAccessors(_ns, _job, _input_table);
	}
	else {
		{
			htConnPool::htSession sess = m_conn_pool->get();
			m_ns = sess.client->namespace_open(_ns);
		}
		createDbAccessors(_ns, _job, _input_table);
		loadStates();
	}
	
	makeRanges();
}

void hSpliterLocal::createDbAccessors(std::string _ns,
								std::string _job,
								std::string _input_table)
{
	//htConnPoolPtr pool0(new htConnPool(*m_conn_pool.get()));
	//htConnPoolPtr pool1(new htConnPool(*m_conn_pool.get()));
	//htConnPoolPtr pool2(new htConnPool(*m_conn_pool.get()));
	m_input_scanner.reset(new htKeyScanner(m_conn_pool, _ns, _input_table));
	m_states_scanner.reset(new htCollScanner(m_conn_pool, _ns, _job, "handled"));
	m_writer.reset(new htCollWriterConc(m_conn_pool, _ns, _job));
}

void hSpliterLocal::loadStates()
{
	int loaded = 0;
	while (!m_states_scanner->end()) {
		KeyValue cell = m_states_scanner->getNextCell();
		m_keys_handled.insert(std::pair<std::string, bool> \
						(cell.key, stringToBool(cell.value) ));
		loaded++;
		//m_nhandled++;
		//std::cout << "loaded state: " << stringToBool(cell.value) << std::endl;
	}
	std::cout << "hSpliterLocal::loadStates loaded: " << loaded << std::endl;
}

/*
bool hSpliterLocal::isOwned(std::string key)
{
	std::tr1::unordered_map<std::string, KeyState>::iterator it = \
		m_key_states.find(key);
	
	if (it != m_key_states.end())
	{
		return it->second.owned;
	}
	else
	{
		return false;
	}	
}
*/

bool hSpliterLocal::isCommiting(std::string key)
{
	std::tr1::unordered_map<std::string, uint64_t>::iterator it = 
			m_keys_commiting.find(key);
	if (it == m_keys_commiting.end()) {
		return false;
	}
	else {
		return true;
	}
}

bool hSpliterLocal::isHandled(const std::string &key)
{
	std::tr1::unordered_map<std::string, bool>::iterator it = \
		m_keys_handled.find(key);
	
	if (it != m_keys_handled.end()) {
		return it->second;
	}
	else {
		return false;
	}
}

void hSpliterLocal::makeRanges()
{
	std::cout << "hSpliterLocal::makeRanges\n";
	bool beg_set = 0;
	std::string beg_key, end_key;
	size_t nkeys_in_step;
	m_nkeys = 0;
	std::string key;
	
	//m_input_scanner->reset();
	while (!m_input_scanner->end()) {
		key = m_input_scanner->getNextKey();
		//std::cout << "key: " << key << std::endl;
		//m_keys_handled.insert(std::pair<std::string,bool>(key, false));
		m_nkeys++;
		
		if (m_nkeys % 1000 == 0)
			//std::cout << "key: " << key << " n: " << m_nkeys << std::endl;
			continue;
		
		if (!isHandled(key)) {
			if (!beg_set) {
				beg_key = key;
				beg_set = 1;
				nkeys_in_step++;
			}
			else {			
				end_key = key;
				nkeys_in_step++;
				if (nkeys_in_step >= m_key_step) {
					//std::cout << "range: " << beg_key << " " << end_key << std::endl;
					m_free_ranges.push(KeyRange(beg_key, end_key));
					beg_set = 0;
					nkeys_in_step = 0;
				}
			}
		}
		else {
			m_nhandled++;
			if (beg_set) {
				m_free_ranges.push(KeyRange(beg_key, end_key));
				beg_set = 0;
			}
		}
		
		if (m_nkeys % 10 == 0) 
			std::cout << "looked keys: " << m_nkeys << " free ranges: " <<
					m_free_ranges.size() << std::endl; 
	}
	if (beg_set) {
		//std::cout << "range: " << beg_key << " " << key << std::endl;
		m_free_ranges.push(KeyRange(beg_key, key));
	}
	std::cout << "made " << m_free_ranges.size() << " ranges\n";
	std::cout << "makeRanges finished\n";
}

KeyRange hSpliterLocal::getSplit()
{
	hLockTicketPtr lock_ticket = m_lock->lock();
	if (m_free_ranges.empty()) {
		std::cout << "________hSpliterLocal::getSplit MakeRanges()\n"; 
		makeRanges();
	}
	
	if (!m_free_ranges.empty()) {
		KeyRange range = m_free_ranges.front();
		m_free_ranges.pop();	
		return range;
		
	} else
		return KeyRange::getEmptyRange();	
	
	/*
		// find not handled ranges
		size_t nscans = 0;
		while (true) {
			std::string key_beg;
			std::string key_end;
			bool beg_set = false;
			size_t nkeys = 0;

			while (!m_input_scanner->end()) {
				std::string key = m_input_scanner->getNextKey();
				if (!isHandled(key)) {
					if (beg_set) {
						nkeys++;
						key_end = key;
						if (nkeys >= m_key_step)
							break;
					}
					else {
						key_beg = key_end = key;
						beg_set = true;
					}
				}
				else {
					if (beg_set) {
						key_end = key;
						break;
					}
				}
			}

			if (beg_set) {
				return KeyRange(key_beg, key_end);
			}
			else {
				nscans++;
				if (nscans>2) {
					std::cout << "handled " << m_nhandled 
							<< " of " << m_nkeys << std::endl; 
					return KeyRange("b", "a");
				}
				m_input_scanner->reset();
			}
		}
	}*/
}

bool hSpliterLocal::tryKeyCommit(std::string key)
{
	hLockTicketPtr lock_ticket = m_lock->lock();
	// trylock key
	std::tr1::unordered_map<std::string, uint64_t>::iterator it = 
			m_keys_commiting.find(key);
	if (it != m_keys_commiting.end()) {
		return false;
	}
	
	m_keys_commiting.insert(std::pair<std::string, uint64_t>(key, time(0)));
	
	return true;
}

void hSpliterLocal::setKeyCommited(std::string key)
{
	hLockTicketPtr lock_ticket = m_lock->lock();
	std::tr1::unordered_map<std::string, bool>::iterator it = \
		m_keys_handled.find(key);
	
	if (it != m_keys_handled.end()) {
		it->second = true;
	}
	else {
		m_keys_handled.insert(std::pair<std::string, bool>(key, true));
	}
	m_nhandled++;
	lock_ticket->unlock();
	
	m_writer->insertSync(KeyValue(key, "1"), "handled");
}

hSpliterLocal::~hSpliterLocal()
{
}
