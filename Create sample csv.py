import pandas as pd
import numpy as np
from collections import Counter
import constants as cons

train = pd.read_csv('train.csv', index_col = 'id')
test = pd.read_csv('test.csv', index_col = 'id')
train['fault_severity'] = train['fault_severity'].apply(lambda x: int(x))

data = pd.concat([train, test], axis = 0).fillna('predict!')
data = data.sort_index() 

event_type = pd.read_csv('event_type.csv')
log_feature = pd.read_csv('log_feature.csv')
resource_type = pd.read_csv('resource_type.csv')
severity_type = pd.read_csv('severity_type.csv')

id_order = severity_type.id.values

event_type['event_type'] = event_type['event_type'].apply(lambda x: int(x.split(' ')[1]))
log_feature['log_feature'] = log_feature['log_feature'].apply(lambda x: int(x.split(' ')[1]))
resource_type['resource_type'] = resource_type['resource_type'].apply(lambda x: int(x.split(' ')[1]))
severity_type['severity_type'] = severity_type['severity_type'].apply(lambda x: int(x.split(' ')[1]))
data['location'] = data['location'].apply(lambda x: int(x.split(' ')[1]))

events = pd.get_dummies(event_type['event_type'], prefix = 'e')
events = pd.concat([event_type['id'], events], axis = 1)
events = events.groupby(['id']).sum()

logs = pd.get_dummies(log_feature['log_feature'], prefix = 'l')
logs = logs.multiply(log_feature['volume'].values, axis = 0)
logs = pd.concat([log_feature['id'], logs], axis = 1)
logs = logs.groupby(['id']).sum()

resources = pd.get_dummies(resource_type['resource_type'], prefix = 'r')
resources = pd.concat([resource_type['id'], resources], axis = 1)
resources = resources.groupby(['id']).sum()

severity = pd.get_dummies(severity_type['severity_type'], prefix = 's')
severity = pd.concat([severity_type['id'], severity], axis = 1)
severity = severity.groupby(['id']).sum()

merge_data = pd.concat([data, events, resources, severity, logs], axis = 1)
merge_data = merge_data.reindex(id_order)
merge_data['intra-location num'] = merge_data.groupby('location').cumcount()
merge_data['intra-location fraction'] = merge_data.groupby('location')['intra-location num'].apply(lambda x: x / (x.max() + 1))
merge_data.drop('intra-location num', axis = 1, inplace = True)
merge_data.to_csv('sample.csv')