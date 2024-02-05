from neo4j import GraphDatabase, basic_auth

url="bolt://localhost:7687"
username='neo4j'
password='password'
driver = GraphDatabase.driver(url, auth=basic_auth(username, password))

# 执行固定 cql
def runCql(cql):
    with driver.session() as session:
            session.run(cql)
# 执行传多个参数的 cql
def runCql(cql, **kwargs):
    with driver.session() as session:
        session.run(cql, **kwargs)
if __name__ == "__main__":
    cql='''
merge (:person {name:"gbhu",age:25}) -[:header] -> (:etl {name:'bohai',born:2010})
'''
    cql_delete_all='''
match (n) DETACH DELETE n
'''
    # runCql(cql)
    cql_attr1='''
optional match (a:etl {name: $name_from})
create (a)-[:etl]->(b:etl {name: $name_to})
return a,b
'''
    cql_attr2='''
optional match (b:etl {name: $name_to})
create (a:etl {name: $name_from})-[:etl]->(b)
return a,b
'''
    cql_attr3='''
create (a:etl {name: $name_from})-[:etl]->(b:etl {name: $name_to} )
return a,b
'''
    res=runCql(cql_attr3,name_from='a',name_to='f')
    print(res)
    # if runCql(cql_attr1,name_from='a',name_to='f'):
    #     print('cql_attr1')
    # elif runCql(cql_attr2,name_from='a',name_to='f'):
    #     print('cql_attr2')