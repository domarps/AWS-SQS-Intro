### Sending a numpy array as string 

I first started off with a [pil_image](http://pillow.readthedocs.io/en/3.1.x/reference/Image.html#PIL.Image.Image) and converted it to as `np.asarray`.

```python3
array_str = repr(np.asarray(pil_image))
packet = '{}#{}'.format(unique_id, array_str)
```




