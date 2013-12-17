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
	
	m_ns = _client->namespace_open(_ns);
	if (mode == hSpliterClient::START)
	{
		Hypertable::ThriftGen::HqlResult result;
		_client->hql_query(result, m_ns, "drop table if exists "+_job);
		_client->hql_query(result, m_ns, "create table "+_job+\
				" (handled MAX_VERSIONS=1)");
	}

	m_querier.reset(new htQuerier(_client, _ns, _job));
	m_writer.reset(new htCollWriterConc(_client, _ns, _job));
	
	//std::cout << "ns:" << _ns << " input:" << _input_table << std::endl;
	m_input_scanner.reset(new htKeyScanner(_client, _ns, _input_table));
	m_states_scanner.reset(new htCollScanner(_client, _ns, _job, "handled"));
	
	loadStates();
	makeRanges();
}

void hSpliterLocal::loadStates()
{
	while (!m_states_scanner->end())
	{
		KeyValue cell = m_states_scanner->getNextCell();
		m_keys_handled.insert(std::pair<std::string, bool> \
						(cell.key, stringToBool(cell.value) ));
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
	size_t nkeys;
	std::string key;
	
	while (!m_input_scanner->end())
	{
		key = m_input_scanner->getNextKey();
		if (!isHandled(key))
		{
			if (!beg_set)
			{
				beg_key = key;
				beg_set = 1;
				nkeys++;
			}
			else
			{				
				end_key = key;
				nkeys++;
				if (nkeys >= m_key_step)
				{
					//std::cout << "range: " << beg_key << " " << end_key << std::endl;
					m_free_ranges.push(KeyRange(beg_key, end_key));
					beg_set = 0;
					nkeys = 0;
				}
			}
		}
		else
		{
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
	if (!m_free_ranges.empty())
	{
		KeyRange range = m_free_ranges.front();
		m_free_ranges.pop();
		// 
		
		return range;
	}
	else
	{
		return KeyRange("b", "a");
		// reassign keys. find not handled ranges
	}
}

bool hSpliterLocal::tryKeyCommit(std::string key)
{
	// trylock key
}

void hSpliterLocal::setKeyCommited(std::string key)
{
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
	m_writer->insertSync(KeyValue(key, "1"), "handled");
}

hSpliterLocal::~hSpliterLocal()
{
}
