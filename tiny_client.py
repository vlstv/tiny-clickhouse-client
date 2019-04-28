import requests 


class ClickHouse():

    host = 'https://localhost:8123'
    user = 'user'
    password = 'password'

    def clickhouse(self, query):

        params = (
            ('add_http_cors_header', '1'),
            ('log_queries', '1'),
            ('output_format_json_quote_64bit_integers', '1'),
            ('output_format_json_quote_denormals', '1'),
            ('user', '%s' % self.user),
            ('password', '%s' % self.password),
            ('database', 'default'),
            ('result_overflow_mode', 'throw'),
            ('max_result_rows', '5000'),
            ('timeout_overflow_mode', 'throw'),
            ('max_execution_time', '500')
        )

        data = '%s FORMAT JSON ' % (query)
        response = requests.post(self.host, params=params, data=data)
        data = response.json()
        return data['data']