# airflow-xcom-example
An example, with explanation, of passing values through an airflow DAG
<br>
### Issues I had trying to pass values from one task to another
<br><br>
There is a frighteningly minimal amount of documentation that offers a specific example of how to pass a value from one task to another. I want to document a working example so that, in the future, when I have this problem again, I am able to solve it faster than I did this time.
<br><br>
Each Python function is passed \*\*kwargs, to access the DAG's metadata. This metadata holds cross-communication data ("xcom"), which can send, through pushes, and receive, through pulls. In either case, a task instance must be initialized in the function, with ```ti = kwargs['ti']```. To push from a task with ```task_id='Generic_Task_ID'```, use ```<ti.xcom_push(key='unique_identifier', value='value_to_be_passed')```. To pull, use ```variable = ti.xcom_pull(key='unique_identifier', task_ids='Generic_Task_ID'```. Ta-da! Your value from one task is available to another. I pray for my future using Airflow.
<br><br>
### A note on return
Within their respective Python functions, these values *can* be simply returned, and will still be included in the xcom metadata (by default, unless your configuration specifies otherwise). However, this is *not* as intuitive as using the xcom_push() method, and does not demonstrate a clear, reciprocal function to xcom_pull(), which *is* required to obtain the data after returning or pushing it. Additionally, when just returning, the data is stored under the task id with the key 'returned_value', which is both vague and singular - if you wanted to push more than one value, you would have to use xcom_push(), so it intuitively makes sense to me to adopt it as the default syntax.
