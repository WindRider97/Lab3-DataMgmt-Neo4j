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
                popup=f"{record['c']['name']}<br>"
                      f"{record['c']['population']}",
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

    def display_cities_within_distance(self):
        map_1 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.execute_read(self._display_cities_within_distance, map_1)
        map_1.save('out/2.2.html')

    def _display_cities_within_distance(self, tx, m):
        query_within_distance = (
        """
        MATCH (startCity:City {name: "Luzern"})-[:CONNECTED*1..4]-(connectedCity:City)
        WHERE connectedCity.population > 10000
        RETURN DISTINCT connectedCity
        """
        )
        cities_query = "MATCH (c:City) RETURN c"
        lines_query = "MATCH (c1:City)-[r:CONNECTED]-(c2:City) RETURN c1, c2, r"

        cities_result = tx.run(cities_query)
        lines_result = tx.run(lines_query)
        distance_result = tx.run(query_within_distance)

        cities_near_Luzern = [record['connectedCity']['name'] for record in distance_result]
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
        for record in cities_result:
            city_name = record['c']['name']
            color = (
                "#00b300" if city_name == "Luzern" else
                "#00e600" if city_name in cities_near_Luzern else
                "#3186cc"
            )
            display_city_on_map(
                m=m,
                popup=f"{city_name}<br>"
                      f"{record['c']['population']} habitants",
                latitude=record['c']['latitude'],
                longitude=record['c']['longitude'],
                color=color
            )

    def display_shortest_path_km(self):
        map_1 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.execute_read(self._display_shortest_path_km, map_1)
        map_1.save('out/2.3.1.html')

    @staticmethod
    def _display_shortest_path_km(tx, m):
        km_graph_query = (
            """
            CALL gds.graph.create(
            'shortest_path_km_graph',
            'City',
            {
                CONNECTED: {
                type: 'CONNECTED',
                orientation: 'UNDIRECTED',
                properties: {
                    km: {
                    property: 'km',
                    defaultValue: 1.0
                    }
                }
                }
            }
            )
            """
        )
        shortest_km_query = (
            """
            MATCH (source:City {name: 'Geneve'}), (target:City {name: 'Chur'})
            CALL gds.shortestPath.dijkstra.stream('shortest_path_km_graph', {
                sourceNode: source,
                targetNode: target,
                relationshipWeightProperty: 'km'
            })
            YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
            RETURN
                index,
                gds.util.asNode(sourceNode).name AS sourceNodeName,
                gds.util.asNode(targetNode).name AS targetNodeName,
                totalCost,
                [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
                costs,
                nodes(path) as path
            ORDER BY index
            """
        )
        km_graph_result = tx.run(km_graph_query)
        shortest_km_result = tx.run(shortest_km_query)
        cities_query = "MATCH (c:City) RETURN c"
        lines_query = "MATCH (c1:City)-[r:CONNECTED]-(c2:City) RETURN c1, c2, r"

        cities_result = tx.run(cities_query)
        lines_result = tx.run(lines_query)

        shortest_path_cities = [record['nodeNames'] for record in shortest_km_result]   
        shortest_path_cities = [item for sublist in shortest_path_cities for item in sublist]

        for record in lines_result:
            city1_coords = (record['c1']['latitude'], record['c1']['longitude'])
            city2_coords = (record['c2']['latitude'], record['c2']['longitude'])
            
            color = (
                "#ff0000" if record['c1']['name'] in shortest_path_cities and record['c2']['name'] in shortest_path_cities else
                "#3186cc"
            )

            display_polyline_on_map(
                m=m,
                locations=[city1_coords, city2_coords],
                popup=f"Connection: {record['c1']['name']} - {record['c2']['name']}<br>"
                      f"Distance: {record['r']['km']} km<br>"
                      f"Tracks: {record['r']['nb_tracks']}<br>"
                      f"Time: {record['r']['time']}",
                color=color
            )
        for record in cities_result:
            city_name = record['c']['name']
            color = (
                "#620000" if city_name in shortest_path_cities else
                "#3186cc"
            )
            
            display_city_on_map(
                m=m,
                popup=f"{city_name}<br>"
                      f"{record['c']['population']} habitants",
                latitude=record['c']['latitude'],
                longitude=record['c']['longitude'],
                color=color
            )


    def display_shortest_path_time(self):
        map_1 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.execute_read(self._display_shortest_path_time, map_1)
        map_1.save('out/2.3.2.html')

    @staticmethod
    def _display_shortest_path_time(tx, m):
        time_graph_query = (
            """
            CALL gds.graph.create(
            'shortest_path_time_graph',
            'City',
            {
                CONNECTED: {
                type: 'CONNECTED',
                orientation: 'UNDIRECTED',
                properties: {
                    time: {
                    property: 'time',
                    defaultValue: 1.0
                    }
                }
                }
            }
            )
            """
        )
        shortest_time_query = (
            """
            MATCH (source:City {name: 'Geneve'}), (target:City {name: 'Chur'})
            CALL gds.shortestPath.dijkstra.stream('shortest_path_time_graph', {
                sourceNode: source,
                targetNode: target,
                relationshipWeightProperty: 'time'
            })
            YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
            RETURN
                index,
                gds.util.asNode(sourceNode).name AS sourceNodeName,
                gds.util.asNode(targetNode).name AS targetNodeName,
                totalCost,
                [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
                costs,
                nodes(path) as path
            ORDER BY index
            """
        )
        tx.run(time_graph_query)
        shortest_time_result = tx.run(shortest_time_query)
        cities_query = "MATCH (c:City) RETURN c"
        lines_query = "MATCH (c1:City)-[r:CONNECTED]-(c2:City) RETURN c1, c2, r"

        cities_result = tx.run(cities_query)
        lines_result = tx.run(lines_query)

        shortest_path_cities = [record['nodeNames'] for record in shortest_time_result]   
        shortest_path_cities = [item for sublist in shortest_path_cities for item in sublist]

        for record in lines_result:
            city1_coords = (record['c1']['latitude'], record['c1']['longitude'])
            city2_coords = (record['c2']['latitude'], record['c2']['longitude'])
            
            color = (
                "#ff0000" if record['c1']['name'] in shortest_path_cities and record['c2']['name'] in shortest_path_cities else
                "#3186cc"
            )

            display_polyline_on_map(
                m=m,
                locations=[city1_coords, city2_coords],
                popup=f"Connection: {record['c1']['name']} - {record['c2']['name']}<br>"
                      f"Distance: {record['r']['km']} km<br>"
                      f"Tracks: {record['r']['nb_tracks']}<br>"
                      f"Time: {record['r']['time']}",
                color=color
            )
        for record in cities_result:
            city_name = record['c']['name']
            color = (
                "#620000" if city_name in shortest_path_cities else
                "#3186cc"
            )
            
            display_city_on_map(
                m=m,
                popup=f"{city_name}<br>"
                      f"{record['c']['population']} habitants",
                latitude=record['c']['latitude'],
                longitude=record['c']['longitude'],
                color=color
            )

    def display_minimum_spanning_tree(self):
        map_1 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.execute_write(self._display_minimum_spanning_tree, map_1)
        map_1.save('out/2.4.html')

    @staticmethod
    def _display_minimum_spanning_tree(tx, m):
        minimum_spanning_tree_query = (
            """
            MATCH (n:City {name: 'Bern'})
            CALL gds.alpha.spanningTree.minimum.write({
                nodeProjection: 'City',
                relationshipProjection: {
                    CONNECTED: {
                        type: 'CONNECTED',
                        orientation: 'UNDIRECTED',
                        properties: {
                            cost: {
                                property: 'cost',
                                defaultValue: 1.0
                            }
                        }
                    }
                },
                startNodeId: id(n),
                relationshipWeightProperty: 'cost',
                writeProperty: 'MINST',
                weightWriteProperty: 'writeCost'
            })
            YIELD createMillis, computeMillis, writeMillis, effectiveNodeCount
            RETURN createMillis, computeMillis, writeMillis, effectiveNodeCount
            """
        )
        
        tx.run(minimum_spanning_tree_query)
        
        path_query = (
            """
            MATCH path = (n:City {name: 'Bern'})-[:MINST*]-()
            WITH relationships(path) AS rels
            UNWIND rels AS rel
            WITH DISTINCT rel AS rel
            RETURN startNode(rel) AS source, endNode(rel) AS target, rel.writeCost AS cost
            """
        )
        
        path_result = tx.run(path_query)
        
        # Draw the minimum spanning tree
        for record in path_result:
            city1_coords = (record['source']['latitude'], record['source']['longitude'])
            city2_coords = (record['target']['latitude'], record['target']['longitude'])
            
            display_polyline_on_map(
                m=m,
                locations=[city1_coords, city2_coords],
                popup=f"Connection: {record['source']['name']} - {record['target']['name']}<br>"
                      f"Distance: {record['cost']} km<br>",
                color="#3186cc"
            )
        
        # Draw the cities
        cities_query = "MATCH (c:City) RETURN c"
        cities_result = tx.run(cities_query)
        for record in cities_result:
            display_city_on_map(
                m=m,
                popup=f"{record['c']['name']}<br>"
                      f"{record['c']['population']} habitants",
                latitude=record['c']['latitude'],
                longitude=record['c']['longitude'],
                color="#3186cc"
            )

if __name__ == "__main__":
    display_train_network = DisplayTrainNetwork("neo4j://localhost:7687")

    center_switzerland = [46.800663464, 8.222665776]

    # display cities on the map
    display_train_network.display_cities()
    # Exercice 2.1
    display_train_network.display_cities_and_lines()
    # Exercice 2.2
    display_train_network.display_cities_within_distance()
    # Exercice 2.3
    display_train_network.display_shortest_path_km()
    display_train_network.display_shortest_path_time()
    # Exercice 2.4
    display_train_network.display_minimum_spanning_tree()