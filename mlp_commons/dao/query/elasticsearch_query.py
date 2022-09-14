class ElasticsearchQuery:
    def create_query_filter(self, query_type, value=None, key=None, gte=None, lte=None, boost=None,
                            case_insensitive=None):
        if query_type == 'term':
            return dict([key, dict([('value', value), ('boost', boost)])])
        if query_type == 'range':
            return dict([(key, dict([('gte', gte), ('lte', lte), ('boost', boost)]))])
        if query_type == 'match':
            return dict([(key, dict([('query', value)]))])
        if query_type == 'regexp':
            return dict([key, dict([('value', value), ('case_insensitive', case_insensitive)])])

    def create_query(self, query_type, filters):
        sub_query = dict([(query_type, filters)])
        query = dict([('query', sub_query)])
        return query
