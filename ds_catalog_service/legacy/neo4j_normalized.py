from neo4j import GraphDatabase

#we'll add proper backend APIs on this later
class Neo4j_NormalizedBackend:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
        
    