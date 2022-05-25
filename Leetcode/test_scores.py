import pandas as pd
from pandas.core.indexes.base import Index

students = [
    {'student_id': 1, 'name': 'Michael Bluth', 'current_grade_level': '3'},
    {'student_id': 2, 'name': 'Lucille Austero', 'current_grade_level': '5'},
    {'student_id': 3, 'name': 'Maeby Funke', 'current_grade_level': '4'},
    {'student_id': 4, 'name': 'Robert Loblaw', 'current_grade_level': '5'},
    {'student_id': 5, 'name': 'Ann Veal', 'current_grade_level': '2'},
    {'student_id': 6, 'name': 'Oscar Bluth', 'current_grade_level': '5'},
    {'student_id': 7, 'name': 'Steven Holt', 'current_grade_level': 'K'},
    {'student_id': 8, 'name': 'Lindsay Funke', 'current_grade_level': '2'},
    {'student_id': 9, 'name': 'Gene Parmesan', 'current_grade_level': '5'},
    {'student_id': 10, 'name': 'Carl Weathers', 'current_grade_level': '3'}
]

benchmark_levels = [
    {'grade_level': 'K', 'text_level_min': 'A', 'text_level_max': 'D'},
    {'grade_level': '1', 'text_level_min': 'E', 'text_level_max': 'J'},
    {'grade_level': '2', 'text_level_min': 'K', 'text_level_max': 'M'},
    {'grade_level': '3', 'text_level_min': 'N', 'text_level_max': 'P'},
    {'grade_level': '4', 'text_level_min': 'Q', 'text_level_max': 'S'},
    {'grade_level': '5', 'text_level_min': 'T', 'text_level_max': 'V'}
]

test_scores = [
    {'student_id': 1, 'text_level': 'O', 'test_score': 'Independent', 'test_date': '2018-09-03'}, 
    {'student_id': 1, 'text_level': 'P', 'test_score': 'Instructional', 'test_date': '2018-09-04'}, 
    {'student_id': 2, 'text_level': 'T', 'test_score': 'Independent', 'test_date': '2018-03-03'}, 
    {'student_id': 2, 'text_level': 'T', 'test_score': 'Instructional', 'test_date': '2018-09-05'}, 
    {'student_id': 2, 'text_level': 'S', 'test_score': 'Instructional', 'test_date': '2018-09-06'}, 
    {'student_id': 3, 'text_level': 'Q', 'test_score': 'Independent', 'test_date': '2018-09-04'}, 
    {'student_id': 3, 'text_level': 'R', 'test_score': 'Independent', 'test_date': '2018-09-04'}, 
    {'student_id': 3, 'text_level': 'R', 'test_score': 'Independent', 'test_date': '2018-09-05'}, 
    {'student_id': 3, 'text_level': 'S', 'test_score': 'Instructional', 'test_date': '2018-09-05'}, 
    {'student_id': 4, 'text_level': 'P', 'test_score': 'Independent', 'test_date': '2018-09-06'}, 
    {'student_id': 4, 'text_level': 'Q', 'test_score': 'Instructional', 'test_date': '2018-09-07'}, 
    {'student_id': 5, 'text_level': 'L', 'test_score': 'Independent', 'test_date': '2018-09-06'}, 
    {'student_id': 5, 'text_level': 'N', 'test_score': 'Instructional', 'test_date': '2018-09-06'}, 
    {'student_id': 5, 'text_level': 'N', 'test_score': 'Instructional', 'test_date': '2018-09-07'}, 
    {'student_id': 5, 'text_level': 'M', 'test_score': 'Instructional', 'test_date': '2018-09-07'}, 
    {'student_id': 6, 'text_level': 'T', 'test_score': 'Instructional', 'test_date': '2018-09-05'}, 
    {'student_id': 6, 'text_level': 'S', 'test_score': 'Independent', 'test_date': '2018-09-06'}, 
    {'student_id': 7, 'text_level': 'D', 'test_score': 'Instructional', 'test_date': '2018-09-13'}, 
    {'student_id': 7, 'text_level': 'C', 'test_score': 'Instructional', 'test_date': '2018-09-13'}, 
    {'student_id': 7, 'text_level': 'B', 'test_score': 'Instructional', 'test_date': '2018-09-15'}, 
    {'student_id': 8, 'text_level': 'A', 'test_score': 'Instructional', 'test_date': '2018-09-04'}, 
    {'student_id': 8, 'text_level': 'B', 'test_score': 'Instructional', 'test_date': '2018-09-04'}, 
    {'student_id': 8, 'text_level': 'C', 'test_score': 'Independent', 'test_date': '2018-09-04'}, 
    {'student_id': 8, 'text_level': 'C', 'test_score': 'Independent', 'test_date': '2018-09-06'}, 
    {'student_id': 9, 'text_level': 'U', 'test_score': 'Instructional', 'test_date': '2018-09-17'}, 
    {'student_id': 9, 'text_level': 'T', 'test_score': 'Independent', 'test_date': '2018-09-18'}, 
    {'student_id': 10, 'text_level': 'M', 'test_score': 'Independent', 'test_date': '2018-09-22'}, 
    {'student_id': 10, 'text_level': 'N', 'test_score': 'Instructional', 'test_date': '2018-09-22'}, 
    {'student_id': 10, 'text_level': 'O', 'test_score': 'Frustrational', 'test_date': '2018-09-22'}
]

'''
QUESTION 1: Check for Understanding

To ensure you understand the concepts behind this exercise, start by finding each student's Independent Reading Level. Your results should look something like this (please print your result to the console):

student_id | name          | independent_reading_level
+----------+---------------+--------------------------+
| 1        | Michael Bluth | O                        |
...
| 10       | Carl Weathers | M                        |
+----------+---------------+--------------------------+
'''

#Your code here
df_students = pd.DataFrame(students)
df_test_scores =pd.DataFrame(test_scores)
T1 = pd.merge(df_students, df_test_scores, on='student_id', how='left')
T2 = T1[['student_id','name','text_level']].loc[T1['test_score'] == 'Independent']
result = T2.rename({'text_level':'independent_reading_level'},axis=1)
print (result.to_markdown(index=False))


''' 
QUESTION 2: Percent Reading on Grade Level

Find the percent of students currently *reading on grade level* for each grade level. Your results should look something like this: 

grade_level | percent_reading_on_GL
+-----------+----------------------+
| K         + ?                    |
| 2         + ?                    |
| 3         + ?                    |
| 4         + ?                    |
| 5         + ?                    |
+-----------+----------------------+

'''

#Your code here
T3 = df_students[['student_id','current_grade_level']].loc[df_students['current_grade_level'] == 'K']
#T3['%'] = 100 * T3['current_grade_level'] / T3.groupby('student_id')['current_grade_level'].transform('sum')
#print(T3.sort_values(['current_grade_level']).reset_index(drop=True))

df = df_students['current_grade_level'].unique()
dataframe=pd.DataFrame(df, columns=['grade_level']) 
#df['grade_level'] = df
df1= df_students.groupby('current_grade_level')['student_id'].nunique() 
df3 = df_students[df_students.columns[0]].count()
dataframe['%' ]= 100 * df_students.groupby('current_grade_level')['student_id'].nunique() / df_students[df_students.columns[0]].count()

print(df)
print(df1)
print(df3)
print(dataframe)



''' 
QUESTION 3: Students in Need of Additional Support

Find the list of students (id, first_name, last_name) reading more than 1 grade level below their current grade level. (e.g., a 4th grader reading at-or-below a 2nd grade reading level would be included in this list)

id  | name      |
+---+-----------+
| ? | ?         |
+---+-----------+

'''

#Your code here



