# MPIFile

An API based on Flask, it receives files, shurd and send chunks to its nodes via MPI.

<img src="Architecture.png" title="Architecture" width="500" height="250">

---

## Server

```
mpirun -n <nodes> python3 main.py
```

## Client

```
python3 client.py

1) upload
2) list
3) Download
4) remove
5) exit
Choice:
```

---

## UML Diagram:

<img src="classes_UML.png" title="classes_UML" width="600" height="500">
<img src="packages_UML.png" title="packages_UML" width="900" height="200">
