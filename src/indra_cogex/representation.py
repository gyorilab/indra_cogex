__all__ = ['Node', 'Relation']


class Node:
    def __init__(self, identifier, labels, data=None):
        self.identifier = identifier
        self.labels = labels
        self.data = data if data else {}

    def __str__(self):
        data_str = (', '.join(['%s:\'%s\'' % (k, v)
                    for k, v in [('id', self.identifier)] +
                               list(self.data.items())]))
        labels_str = ':'.join(self.labels)
        return f'(:{labels_str} {data_str})'

    def __repr__(self):
        return str(self)


class Relation:
    def __init__(self, source_id, target_id, labels, data=None):
        self.source_id = source_id
        self.target_id = target_id
        self.labels = labels
        self.data = data if data else {}

    def __str__(self):
        data_str = (', '.join(['%s:\'%s\'' % (k, v)
                               for k, v in self.data.items()]))
        labels_str = ':'.join(self.labels)
        return f'({self.source_id})-[:{labels_str} {data_str}]->' \
               f'({self.target_id})'

    def __repr__(self):
        return str(self)
