from neo4j import GraphDatabase
import pandas as pd


class GenerateTrainNetwork:

    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri)

    def close(self):
        self.driver.close()

    def detach_delete(self):
        with self.driver.session() as session:
            session.execute_write(self._detach_delete)



    @staticmethod
    def _detach_delete(tx):
        query = (
            """
            MATCH(n) DETACH DELETE n;
            """
        )
        tx.run(query)
        query = (
            """
            CALL gds.graph.drop('shortest_path_km_graph')
            """
        )
        tx.run(query)
        query = (
            """
            CALL gds.graph.drop('shortest_path_time_graph')
            """
        )
        tx.run(query)

        print("DETACH DELETE complete")



if __name__ == "__main__":
    generate_train_network = GenerateTrainNetwork("neo4j://localhost:7687")
    generate_train_network.detach_delete()