
![Contact_Sequence](https://github.com/D4rkisek/contact-sequence-diagram/assets/106534376/584f046d-1d1c-40b2-a70f-a31bf415e109)


# Dependencies:
Matplotlib & Raphtory

# Class:
`ContactSequenceDiagram(self, graph, earliest_time=None, latest_time=None, figsize=(12, 5), save_file=False, filename='Contact_Sequence.PNG')`

# Parameters: 
`graph`: Takes a graph object made in Raphtory library. A Graph object containing vertices and edges with temporal information.

`earliest_time` (optional): Takes an integer. The earliest time point to consider in the diagram. Defaults to the earliest time in the graph.

`latest_time` (optional): Takes an integer. The latest time point to consider in the diagram. Defaults to the latest time in the graph.

`figsize` (optional): Takes a tuple of integers. The size of the diagram.

`save_file` (optional): Takes a boolean value. The specification of whether the diagram would want to be saved.

`filename` (optional): Takes a string. The filename for saving the generated diagram.

# Example usage:
```
from raphtory import Graph

g = Graph()
# Add vertices and edges to the graph

# Create and display the diagram
diagram = ContactSequenceDiagram(g)
diagram.create_diagram()
```

# Note:
The program will still process all the data inside the graph even when `earliest_time` and/or `latest_time` is specified.
