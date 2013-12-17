#include "hspliter_tests.h"

void hSpliterTests::testLocal()
{
	ThriftClientPtr client( new ThriftClient("localhost", 38080));
	const std::string ns_name = "test";
	Hypertable::ThriftGen::Namespace ns = client->namespace_open(ns_name);
	
	Hypertable::ThriftGen::HqlResult result;
	client->hql_query(result, ns, "drop table if exists split_input");
	client->hql_query(result, ns, "create table split_input (num MAX_VERSIONS=1)");
	client->close_namespace(ns);
	
	htCollWriterConc writer(client, ns_name, "split_input");
	
	uint32_t control_summ;
	for (int i = 0; i<100; i++)
	{
		char bf[255];
		sprintf(bf, "%d", i);
		writer.insertSync(KeyValue(bf, bf), "num");
		
		control_summ += i;
	}
	
	sleep(1);
	hSpliterLocal spliter(client,
					 ns_name,
					"split_input",
					"split_job",
					hSpliterClient::START,
					20);
	
}
