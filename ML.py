import pandas as pd
from loguru import logger
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger.add('logs/logs.log', level='DEBUG', rotation="500 MB")

class ML:

    def __init__(self, df_action, df_event):
        self.df_action = df_action
        self.df_event = df_event
        self.features = ['country', 'city', 'category', 'subcategory1',
                'subcategory2', 'subcategory3', 'target_age_group']

    def combine_features(self, row):
        result = ''
        for key in self.features:
            result += str(row[key]) + " "
        return result


    def get_events(self):
        result = []

        for feature in self.features:
            # self.df_action[feature] = self.df_action[feature].fillna('')
            self.df_event[feature] = self.df_event[feature].fillna('')
        self.df_event["combined_features"] = self.df_event.apply(self.combine_features, axis=1)
        # self.df_action["combined_features"] = self.df_action.apply(self.combine_features, axis=1)

        cv = CountVectorizer()
        try:
            count_matrix = cv.fit_transform(self.df_event["combined_features"])
        except ValueError as e:
            logger.warning(e)
            return []
        cosine_sim = cosine_similarity(count_matrix)
        for action in self.df_action.to_dict('records'):
            event_index = self.df_event.loc[self.df_event['event_id'] == action['event_id']].index
            if event_index.empty:
                continue

            similar_events = list(enumerate(cosine_sim[event_index[0]]))

            sorted_similar_events = sorted(similar_events, key=lambda x: x[1], reverse=True)[1:]

            i = 0
            for element in sorted_similar_events:
                result.append(self.df_event.loc[element[0]].event_id)
                i = i + 1
                if i > 1:
                    break
        return result