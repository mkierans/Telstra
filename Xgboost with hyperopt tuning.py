
# For detailed description of xgboost parameters and general tuning techniques
# http://www.analyticsvidhya.com/blog/2016/03/complete-guide-parameter-tuning-xgboost-with-codes-python/

# For tuning for hyperopt
# https://github.com/hyperopt/hyperopt/wiki/FMin

import pandas as pd, numpy as np
import xgboost as xgb
import constants as cons
import time
from sklearn.cross_validation import train_test_split
from hyperopt import hp, fmin, tpe, STATUS_OK, Trials
import random

start_time = time.time()

data = pd.read_csv('sample.csv', index_col = 'id')
train = data.loc[data.fault_severity != 'predict!'].copy()
test = data.loc[data.fault_severity == 'predict!'].copy()
del data
train.loc[:,'fault_severity'] = train.loc[:, 'fault_severity'].apply(lambda x: int(float(x)))
train_labels = train.fault_severity
test_id = test.index
test.drop('fault_severity', axis = 1, inplace = True)
train.drop('fault_severity', axis = 1, inplace = True)
train_values = train.values

num_rounds_dict = {} # for storing how many rounds to run xgboost for each optimal eta value
max_rounds = 10000
random_seed = 0

def objective(space):
    global random_seed
    global scoring_rounds
    scores = []
    num_rounds = []
    space['objective'] = 'multi:softprob' # used for multiclass classification
    space['num_class'] = 3 # 3 class classification problem
    space['eval_metric'] = 'mlogloss' # how is the machine success evaluated
    for i in xrange(scoring_rounds):
        dtrain, dval, ytrain, yval = train_test_split(train_values, train_labels, test_size = 0.2, random_state = random_seed)
        random_seed += 1
        dtrain = xgb.DMatrix(dtrain, label = ytrain)
        dval = xgb.DMatrix(dval, yval)
        evallist = [(dtrain, 'train'), (dval, 'val')]
        model = xgb.train(space, dtrain, max_rounds, evallist, early_stopping_rounds=120, verbose_eval = False)
        scores.append(model.best_score) # append the best score xgboost got on this data split
        num_rounds.append(model.best_iteration) # how many xgboost rounds was the best score achieved at
        print('Model best score', model.best_score)
    print('model_ave_score:', np.mean(scores), 'eta = ', space['eta'], 'max_depth = ', space['max_depth']) 
#          , 'colsample_bytree = ', space['colsample_bytree'], 'min_child_weight = ', space['min_child_weight'],
#          , 'subsample = ', space['subsample'], 'lambda = ', space['lambda'], 'alpha = ', space['alpha'])
    num_rounds_dict[space['eta']] = np.mean(num_rounds)
    return {'loss': np.mean(scores), 'status': STATUS_OK} # return the average score
    
    
space = {
        'eta': hp.uniform('eta', 0.06, 0.2), # learning rate
        'max_depth': hp.quniform('max_depth', 5, 9, 1), # max depth of a tree
#        'min_child_weight': hp.uniform('min_child_weight', 0.5, 1.0),
#        'colsample_bytree': hp.uniform('colsample_bytree', 0.5, 1.0),
#        'subsample': hp.uniform('subsample', 0.5, 1.5),
#        'lambda': hp.uniform('lambda', 0.75, 1.25), # l1 regularization term
#        'alpha': hp.uniform('alpha', 0, 0.5) #l2 regularization term
        }

# the following will average a number of xgboost models

num_models = 2 # number of xgboost predictions to create
optimization_rounds = 3 # number of different parameter selections
scoring_rounds = 10 # number of rounds to score per parameter selection

preds = [] # list to store xgboost predictions

for i in xrange(num_models):
#    trials = Trials()
    best = fmin(fn = objective, # function to minimize
            space = space, # parameters to optimize
            algo = tpe.suggest, # 
            max_evals = optimization_rounds)
#            trials = trials)
    dtest = xgb.DMatrix(test)
    train = xgb.DMatrix(train_values, train_labels)
    best['objective'] = 'multi:softprob'
    best['num_class'] = 3
    best['eval_metric'] = 'mlogloss'
    model = xgb.train(best, train, int(num_rounds_dict[best['eta']]), verbose_eval = True)
    prediction = model.predict(dtest)
    prediction = pd.DataFrame(prediction, columns = ['predict_' + str(int(x)) for x in set(train_labels)])
    prediction.set_index(test_id, inplace = True)
    prediction.reset_index(inplace = True)
    preds.append(prediction)

accumulative_predictions = pd.concat([i for i in preds], axis = 0)
average_prediction = accumulative_predictions.groupby('id').mean()
average_prediction.to_csv('XGBoostPred.csv')

print("--- %s seconds ---" % (time.time() - start_time))
