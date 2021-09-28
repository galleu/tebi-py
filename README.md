# tebi.py
This allows you to use tebi.io's api easily with python


## Example
```python
from main import Tebi

tebi = Tebi('s3.tebi.io/galleu', auth="<KEY>:<SECRET>")

file = tebi.GetObject("hi.txt")
print(file) # This is the response from the get request.
print(file.content) # the content of hi.txt


# You can also set auth here, if you don't set it
tebi.PutObject("hay.txt", b"Hay there!", auth="TB-PLAIN: <KEY>:<SECRET>")
```