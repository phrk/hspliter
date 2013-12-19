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
	if (_handled=="1")
	{
		return true;
	}
	else if (_handled=="0")
	{
		return false;
	}
	else
	{
		throw "stringToBool undefined value: " + _handled;
	}
}
		
hSpliterLocal::hSpliterLocal(ThriftClientPtr _client,
				std::string _ns,
				std::string _input_table,
				std::string _job,
				hSpliterClient::Mode mode,
				size_t key_step)
{
	m_client = _client;
	m_job = _job;
	m_key_step = key_step;
	m_lock.reset(new hAutoLock);
	m_ns = _client->namespace_open(_ns);
	if (mode == hSpliterClient::START)
	{
		Hypertable::ThriftGen::HqlResult result;
		_client->hql_query(result, m_ns, "drop table if exists "+_job);
		_client->hql_query(result, m_ns, "create table "+_job+\
				" (handled MAX_VERSIONS=1)");
		std::cout << "TABLES DROPPED \n";
		createDbAccessors(_ns, _job, _input_table);
	}
	else
	{
		createDbAccessors(_ns, _job, _input_table);
		loadStates();
	}
	
	makeRanges();
}

void hSpliterLocal::createDbAccessors(std::string _ns,
								std::string _job,
								std::string _input_table)
{
	m_querier.reset(new htQuerier(m_client, _ns, _job));
	m_writer.reset(new htCollWriterConc(m_client, _ns, _job));
	m_input_scanner.reset(new htKeyScanner(m_client, _ns, _input_table));
	m_states_scanner.reset(new htCollScanner(m_client, _ns, _job, "handled"));
}

void hSpliterLocal::loadStates()
{
	while (!m_states_scanner->end())
	{
		KeyValue cell = m_states_scanner->getNextCell();
		m_keys_handled.insert(std::pair<std::string, bool> \
						(cell.key, stringToBool(cell.value) ));
		//std::cout << "loaded state: " << stringToBool(cell.value) << std::endl;
	}
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
	if (it == m_keys_commiting.end())
	{
		return false;
	}
	else
	{
		return true;
	}
}

bool hSpliterLocal::isHandled(std::string key)
{
	std::tr1::unordered_map<std::string, bool>::iterator it = \
		m_keys_handled.find(key);
	
	if (it != m_keys_handled.end())
	{
		return it->second;
	}
	else
	{
		return false;
	}
}

void hSpliterLocal::makeRanges()
{
	bool beg_set = 0;
	std::string beg_key, end_key;
	size_t nkeys_in_step;
	m_nkeys = 0;
	std::string key;
	
	while (!m_input_scanner->end())
	{
		key = m_input_scanner->getNextKey();
		m_keys_handled.insert(std::pair<std::string,bool>(key, false));
		m_nkeys++;
		if (!isHandled(key))
		{
			if (!beg_set)
			{
				beg_key = key;
				beg_set = 1;
				nkeys_in_step++;
			}
			else
			{				
				end_key = key;
				nkeys_in_step++;
				if (nkeys_in_step >= m_key_step)
				{
					//std::cout << "range: " << beg_key << " " << end_key << std::endl;
					m_free_ranges.push(KeyRange(beg_key, end_key));
					beg_set = 0;
					nkeys_in_step = 0;
				}
			}
		}
		else
		{
			m_nhandled++;
			if (beg_set)
			{
				m_free_ranges.push(KeyRange(beg_key, end_key));
				beg_set = 0;
			}
		}
	}
	if (beg_set)
	{
		//std::cout << "range: " << beg_key << " " << key << std::endl;
		m_free_ranges.push(KeyRange(beg_key, key));
	}
}

KeyRange hSpliterLocal::getSplit()
{
	hLockTicket lock_ticket = m_lock->lock();
	if (!m_free_ranges.empty())
	{
		KeyRange range = m_free_ranges.front();
		m_free_ranges.pop();
		// 
		
		return range;
	}
	else
	{
		// reassign keys. find not handled ranges
		std::cout << "trying reassing\n";
		std::tr1::unordered_map<std::string, bool>::iterator it =
				m_keys_handled.begin();
		std::string key_beg;
		std::string key_end;
		bool beg_set = false;
		
		while (it != m_keys_handled.end())
		{
			std::string key = it->first;
			//std::cout << "key " << key << " handled " << it->second << std::endl;
			if (!it->second) //not handled  // && !isCommiting(key))
			{
				if (beg_set)
				{
					key_end = key;
				}
				else
				{
					key_beg = key_end = key;
					beg_set = true;
				}
			}
			else
			{
				//std::cout << "key already handled " << key << std::endl;
				if (beg_set)
				{
					key_end = key;
					break;
				}
			}
			it++;
		}
		
		if (beg_set)
		{
			std::cout << "reassigning " << key_beg << " " << key_end << std::endl;
			return KeyRange(key_beg, key_end);
		}
		
		std::cout << "handled " << m_nhandled << " of " << m_nkeys << std::endl; 
		return KeyRange("b", "a");
		
	}
}

bool hSpliterLocal::tryKeyCommit(std::string key)
{
	hLockTicket lock_ticket = m_lock->lock();
	// trylock key
	std::tr1::unordered_map<std::string, uint64_t>::iterator it = 
			m_keys_commiting.find(key);
	if (it != m_keys_commiting.end())
	{
		return false;
	}
	
	m_keys_commiting.insert(std::pair<std::string, uint64_t>(key, time(0)));
	
	return true;
}

void hSpliterLocal::setKeyCommited(std::string key)
{
	hLockTicket lock_ticket = m_lock->lock();
	std::tr1::unordered_map<std::string, bool>::iterator it = \
		m_keys_handled.find(key);
	
	if (it != m_keys_handled.end())
	{
		it->second = true;
	}
	else
	{
		m_keys_handled.insert(std::pair<std::string, bool>(key, true));
	}
	m_nhandled++;
	lock_ticket.unlock();
	
	m_writer->insertSync(KeyValue(key, "1"), "handled");
}

hSpliterLocal::~hSpliterLocal()
{
}
