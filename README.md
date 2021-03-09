# airflow-xcom-example
An example, with explanation, of passing values through an airflow DAG
<br>
### Issues I had trying to pass values from one task to another
There is a frighteningly minimal amount of documentation that offers a specific example of how to pass a value from one task to another. I want to document a working example so that, in the future, when I have this problem again, I am able to solve it faster than I did this time.
<br><br>
Each Python function is passed \*\*kwargs, to access the DAG's metadata. This metadata holds cross-communication data ("xcom"), which can receive, through pushes, and give, through pulls. In either case, a task instance must be initialized in the function, with ```ti = kwargs['ti']```. To push from a task with ```task_id='Generic_Task_ID'```, use ```<ti.xcom_push(key='unique_identifier', value='value_to_be_passed')```. To pull, use ```variable = ti.xcom_pull(key='unique_identifier', task_ids='Generic_Task_ID'```. Ta-da! Your value from one task is available to another. I pray for my future using Airflow.
