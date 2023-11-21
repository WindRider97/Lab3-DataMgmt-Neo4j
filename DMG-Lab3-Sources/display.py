from neo4j import GraphDatabase
import folium


# display city on the folium map
def display_city_on_map(m, popup, latitude, longitude, radius=1000, color="#3186cc"):
    folium.Circle(
        location=(latitude, longitude),
        radius=radius,
        popup=popup,
        color=color,
        fill=True,
        fill_opacity=0.8,
    ).add_to(m)


# display polyline on the folium map
# locations: (list of points (latitude, longitude)) – Latitude and Longitude of line
def display_polyline_on_map(m, locations, popup=None, color="#3186cc", weight=2.0):
    folium.PolyLine(
        locations,
        popup=popup,
        color=color,
        weight=weight,
        opacity=1
    ).add_to(m)


class DisplayTrainNetwork:

    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri)

    def close(self):
        self.driver.close()

    def display_cities(self):
        map_1 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.execute_read(self._display_cities, map_1)
        map_1.save('out/1.html')

    @staticmethod
    def _display_cities(tx, m):
        query = (
            """
            MATCH (c:City)
            RETURN c
            """
        )
        result = tx.run(query)
        for record in result:
            display_city_on_map(
                m=m,
                popup=record['c']['name'],
                latitude=record['c']['latitude'],
                longitude=record['c']['longitude']
            )

    # Exercice 2.1
    def display_cities_and_lines(self):
        map_1 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.execute_read(self._display_cities_and_lines, map_1)
        map_1.save('out/2.1.html')

    @staticmethod
    def _display_cities_and_lines(tx, m):
        cities_query = "MATCH (c:City) RETURN c"
        lines_query = "MATCH (c1:City)-[r:CONNECTED]-(c2:City) RETURN c1, c2, r"

        cities_result = tx.run(cities_query)
        lines_result = tx.run(lines_query)

        for record in cities_result:
            display_city_on_map(
                m=m,
                popup=record['c']['name'],
                latitude=record['c']['latitude'],
                longitude=record['c']['longitude']
            )

        for record in lines_result:
            city1_coords = (record['c1']['latitude'], record['c1']['longitude'])
            city2_coords = (record['c2']['latitude'], record['c2']['longitude'])

            display_polyline_on_map(
                m=m,
                locations=[city1_coords, city2_coords],
                popup=f"Connection: {record['c1']['name']} - {record['c2']['name']}<br>"
                      f"Distance: {record['r']['km']} km<br>"
                      f"Tracks: {record['r']['nb_tracks']}<br>"
                      f"Time: {record['r']['time']}"
            )

    def display_cities_within_distance(self, source_city_name, max_stops, min_population):
        map_1 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.execute_read(self._display_cities_and_lines, map_1)
            session.execute_read(self._display_cities_within_distance(session, map_1, source_city_name, max_stops, min_population))
        map_1.save('out/2.2.html')

    def _display_cities_within_distance(self, tx, m, source_city_name, max_stops, min_population):
        query_within_distance = (
        """
        MATCH (source:City {name: $source_city_name})-[:CONNECTED*1..$max_stops]-(city:City)
        WHERE city.population > $min_population
        RETURN source, city
        """
        )

        result = tx.run(query_within_distance)
        for record in result:
            display_city_on_map(
                m=m,
                popup=record['city']['name'],
                latitude=record['city']['latitude'],
                longitude=record['city']['longitude'],
                color="#00b300"
            )

# color="#00b300"
# color="#00e600"

if __name__ == "__main__":
    display_train_network = DisplayTrainNetwork("neo4j://localhost:7687")

    center_switzerland = [46.800663464, 8.222665776]

    # display cities on the map
    display_train_network.display_cities()
    # Exercice 2.1
    display_train_network.display_cities_and_lines()
    # Exercice 2.2
    display_train_network.display_cities_within_distance("Lucerne", 4, 10000)