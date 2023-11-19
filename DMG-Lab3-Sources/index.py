from neo4j import GraphDatabase
import pandas as pd


class GenerateTrainNetwork:

    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri)

    def close(self):
        self.driver.close()

    def create_cities(self):
        cities = pd.read_csv('data/cities.csv', sep=';')
        for index, row in cities.iterrows():
            with self.driver.session() as session:
                session.execute_write(
                    self._create_city,
                    row['name'],
                    row['latitude'],
                    row['longitude'],
                    row['population']
                )

    def create_lines(self):
        lines = pd.read_csv('data/lines.csv', sep=';')
        for index, row in lines.iterrows():
            with self.driver.session() as session:
                session.execute_write(
                    self._create_line,
                    row['city1'],
                    row['city2'],
                    row['km'],
                    row['time'],
                    row['nbTracks']
                )

    @staticmethod
    def _create_city(tx, name, latitude, longitude, population):
        query = (
            """
            MERGE (c:City { name: $name })
            ON CREATE SET c.latitude = $latitude, c.longitude = $longitude, c.population = $population
            RETURN c
            """
        )
        result = tx.run(query, name=name, latitude=latitude, longitude=longitude, population=population)

        city_created = result.single()['c']
        print("Created City: {name}".format(name=city_created['name']))

    @staticmethod
    def _create_line(tx, city1, city2, km, time, nb_tracks):
        query = (
            """
            MATCH (c1:City {name: $city1}), (c2:City {name: $city2})
            MERGE (c1)-[r:CONNECTED]-(c2)
            ON CREATE SET r.km = $km, r.time = $time, r.nb_tracks = $nb_tracks
            RETURN c1, c2
            """
        )
        '''
        """
        MATCH (c1:City {name: $city1}), (c2:City {name: $city2})
        MERGE (c1)-[r:CONNECTED]->(c2)
        ON CREATE SET r.km = $km, r.time = $time, r.nb_tracks = $nb_tracks
        MERGE (c2)-[:CONNECTED]->(c1)
        ON CREATE SET r.km = $km, r.time = $time, r.nb_tracks = $nb_tracks
        RETURN c1, c2
        """ ???
        '''
        result = tx.run(query, city1=city1, city2=city2, km=km, time=time, nb_tracks=nb_tracks)

        line_created = result.single()
        print("Created line between: {city1} - {city2}".format(city1=line_created['c1']['name'], city2=line_created['c2']['name']))


if __name__ == "__main__":
    generate_train_network = GenerateTrainNetwork("neo4j://localhost:7687")

    # create all city nodes
    generate_train_network.create_cities()
    # Exercice 2.1
    generate_train_network.create_lines()
