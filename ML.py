import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

df = pd.read_csv("users_action.csv")
features = ['Country', 'City', 'UserAgeGroup', 'Distance',
       'Category', 'SubCategory1', 'SubCategory2', 'SubCategory3',
       'TargetAgeGroup']
def combine_features(row):
    result = ''
    for key in features:
        result += str(row[key])+" "
    return result

for feature in features:
    df[feature] = df[feature].fillna('')
df["combined_features"] = df.apply(combine_features,axis=1)

df['index'] = range(0, len(df))

cv = CountVectorizer()
count_matrix = cv.fit_transform(df["combined_features"])

cosine_sim = cosine_similarity(count_matrix)

def find_title_from_index(index):
    return df[df.index == index]
def find_index_from_title(title):
    return df[df.Category == title]["index"].values[0]


movie = "Food"
movie_index = find_index_from_title(movie)

similar_movies = list(enumerate(cosine_sim[movie_index]))

sorted_similar_movies = sorted(similar_movies,key=lambda x:x[1],reverse=True)[1:]

i=0
for element in sorted_similar_movies:
    print(find_title_from_index(element[0]))
    i=i+1
    if i>10:
        break
# print(movie_index)