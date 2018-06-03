from influxdb import InfluxDBClient
from databaseConnection import DatabaseConnection


class InfluxConnection(DatabaseConnection):

    def __init__(self, db_name, host='localhost', port=8086, user='root', password='root'):
        """Return an InfluxDBClient object which represents a connection to
        an InfluxDBClient object."""
        super(InfluxConnection, self).__init__(db_name)

        try:
            self.client = InfluxDBClient(host, port, user, password)
        except:
            raise ConnectionError(
                "Error connecting to InfluxDB client at: host: " + str(host) + "; on port: " + str(port))

        if not self.db_exists(db_name):
            self.client.create_database(self.db_name)
        self.client.switch_database(self.db_name)

    class Factory:
        def create(self, db_name, host, port, user, password):
            return InfluxConnection(db_name, host, port, user, password)


    def db_exists(self, db_name):
        for db in self.client.get_list_database():
            if db['name'] == db_name:
                return True
        return False

    def query_database(self, query):
        try:
            result = self.client.query(query)
        except Exception as err:
            raise ConnectionError(
                "Error querying InfluxDB client: " + str(err))
        return result

    def get_recorded_measurement_list(self):
        # WARNING: THIS FUNCTION ONLY WORKS FOR NEWER FORMAT OF POLLER OUTPUT (SEE data_sources FOLDER README.TXT FILES)
        measurements = []

        try:
            query_result = self.query_database('SHOW MEASUREMENTS')
        except:
            raise ConnectionError(
                "Error querying InfluxDB client. Check client is running/available")

        for result_part in query_result:
            for measurement in result_part:
                query_str_2 = 'SELECT * FROM ' + measurement['name'] #+ ' WHERE time > now() - ' + str(previous_hours) + 'h'
                query_result_2 = self.query_database(query_str_2)
                list_result = list(query_result_2.get_points(measurement['name']))
                if len(list_result) > 0:
                    measurements.append({'name':measurement['name'], 'href': list_result[0]['href']})

        return measurements

    def import_restful_api_response(self, data_points_json):
        try:
            self.client.write_points(data_points_json, time_precision='s')
        except:
            raise ConnectionError(
                "Error importing to InfluxDB client. Check client is running/available")