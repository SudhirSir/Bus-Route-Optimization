# 🚌 Bus Route Optimization using Apache Spark GraphX

### Distributed Graph Analytics | Scala | Apache Spark | GraphX | Pregel

A Big Data Engineering project that analyzes and optimizes Delhi's public bus transportation network using Apache Spark GraphX. The system models bus stops and routes as a weighted directed graph, identifies major transit hubs, detects inefficient travel paths, and simulates optimized express routes using distributed graph processing algorithms.

---

## 📌 Project Overview

Urban transportation networks often suffer from inefficient routes, unnecessary transfers, and congestion at major transit hubs.

This project uses real-world Delhi GTFS (General Transit Feed Specification) transit data from DTC and DIMTS to build a distributed graph representation of the bus network.

Using Apache Spark GraphX and Scala, the project performs graph analytics, identifies connectivity bottlenecks, calculates distance-weighted shortest paths using the Pregel API, and evaluates the impact of introducing a proposed direct express corridor.

## 🎯 Project Objectives

- Process and analyze real-world public transportation data.
- Build a directed, distance-weighted graph using Spark GraphX.
- Identify highly connected and potentially congested bus stops.
- Calculate shortest paths using the Pregel algorithm.
- Detect inefficient routes and transportation bottlenecks.
- Simulate new express corridors and measure optimization improvements.
- Evaluate Spark execution performance across single-core and multi-core configurations.

---

## 🛠️ Technology Stack

| Category | Technologies |
|---|---|
| Programming Language | Scala 2.12.18 |
| Big Data Framework | Apache Spark 3.5.0 |
| Graph Processing | Spark GraphX |
| Graph Algorithm | Pregel, SSSP |
| Data Processing | Python, Pandas, NumPy |
| Visualization | NetworkX, Matplotlib, Folium |
| Build Tool | sbt |
| Data Format | GTFS, CSV |
| Runtime | JVM / Java 8+ |

---

## 📊 Dataset

**Source:** Delhi Open Transit Data / Mobility Database

Dataset contains public transportation information from Delhi Transport Corporation (DTC) and Delhi Integrated Multi-Modal Transit System (DIMTS).

- [Delhi Open Transit Data Portal](https://otd.delhi.gov.in/)
- [Mobility Database – Delhi GTFS Feed](https://mobilitydatabase.org/feeds/gtfs/mdb-1262)

### Dataset Statistics

| Metric | Value |
|---|---:|
| Total Bus Stops | 8,374 |
| Directed Edges | 8,284 |
| Distinct Bus Routes | 195 |
| Mean Edge Distance | 0.77 km |
| Maximum Edge Distance | 37.95 km |

---

## ⚙️ System Architecture

```text
         Delhi GTFS Dataset
                 |
                 v
       Python Preprocessing
        (Pandas + NumPy)
                 |
                 v
       GTFS Data Cleaning
       and Transformation
                 |
                 v
          stops.csv
          edges.csv
                 |
                 v
       Apache Spark GraphX
           (Scala 2.12)
                 |
        +--------+--------+
        |                 |
        v                 v
  Degree Centrality   Pregel SSSP
        |                 |
        v                 v
  Hub Identification  Shortest Paths
        |                 |
        +--------+--------+
                 |
                 v
        Route Optimization
        (Express Corridor)
                 |
                 v
        Results and Analysis
                 |
                 v
       Python Visualization
       (NetworkX + Folium)
```

---

## 🚀 Key Features

### 1. GTFS Data Preprocessing
- Reads raw GTFS transit files.
- Extracts consecutive bus stop connections.
- Maps stop IDs to numerical vertex IDs.
- Removes duplicate and invalid edges.
- Calculates approximate road distances using the Haversine formula with a 1.3 road winding factor.

### 2. Distributed Graph Construction
- Creates a directed graph using Apache Spark GraphX.
- Represents bus stops as vertices.
- Represents bus connections as weighted edges.
- Uses Spark RDDs and graph partitioning for distributed graph processing.
- Applies graph caching to improve iterative computation.

### 3. Degree Centrality Analysis
Identifies highly connected bus stops using:

- Incoming degree.
- Outgoing degree.
- Total degree.
- Normalized degree centrality.
- Number of distinct routes serving each stop.

### 4. Pregel-Based Shortest Path
Implements a custom distance-weighted Single Source Shortest Path (SSSP) algorithm using Spark GraphX Pregel API.

Unlike simple hop-based shortest paths, the algorithm considers the actual estimated distance of each connection.

### 5. Route Optimization
Simulates a proposed two-way express corridor (Route 9999) between Connaught Place and Dhaula Kuan.

The system compares the original and optimized paths to evaluate distance and transfer reductions.

### 6. Interactive Visualization
Uses:
- NetworkX for network representation.
- Matplotlib for static before-and-after comparisons.
- Folium for interactive geographical maps.

---

## 📈 Results and Performance

### Route Optimization Results

| Metric | Before | After |
|---|---:|---:|
| Route Distance | 18.88 km | 9.57 km |
| Transfer Hops | 25 | 1 |
| Stops in Path | 26 | 2 |
| Distance Reduction | — | 49.3% |

<details>
<summary><b>View Performance Analysis</b></summary>

| Execution Metric | Single Core | Multi-Core |
|---|---:|---:|
| Pregel SSSP | 6.52 sec | 14.89 sec |
| Sampled Evaluation | 128.45 sec | 134.20 sec |
| Total Pipeline | 168.58 sec | 176.64 sec |

The multi-core configuration was slightly slower for this relatively small graph due to synchronization, shuffle overhead, and JVM garbage collection costs.

</details>

### Network Optimization Visualization

The following visualization compares the original Delhi bus network with the proposed optimized express corridor.

![Bus Network Optimization](viz/network.png)

---

## 📂 Project Structure

```text
bus-route-optimization-spark-graphx/
│
├── data/
│   ├── stops.csv
│   └── edges.csv
│
├── gtfs_raw/
│   └── GTFS source files
│
├── output/
│   ├── centrality.csv
│   ├── path_before.csv
│   ├── path_after.csv
│   └── new_edge.csv
│
├── viz/
│   ├── plot.py
│   ├── network.png
│   └── map.html
│
├── preprocess_gtfs.py
├── BusRoute.scala
├── build.sbt
└── README.md
```

---

## 💻 Installation and Execution

### Prerequisites

Install the following:

- Java 8 or compatible JVM.
- Apache Spark 3.5.0.
- Scala 2.12.
- Python 3.x.
- Required Python libraries.

### Step 1: Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/bus-route-optimization-spark-graphx.git

cd bus-route-optimization-spark-graphx
```

### Step 2: Install Python Dependencies

```bash
pip install pandas numpy networkx matplotlib folium
```

### Step 3: Download the Dataset

Download the Delhi GTFS dataset from:

https://mobilitydatabase.org/feeds/gtfs/mdb-1262

Extract the dataset into the `gtfs_raw/` directory.

Ensure the following files are available:

```text
gtfs_raw/
├── stops.txt
├── trips.txt
├── stop_times.txt
├── routes.txt
└── agency.txt
```

### Step 4: Run Data Preprocessing

```bash
python preprocess_gtfs.py
```

This generates:

```text
data/
├── stops.csv
└── edges.csv
```

### Step 5: Run Spark GraphX Analysis

Start Spark Shell:

```bash
spark-shell --master "local[*]"
```

Load the Scala implementation:

```scala
:load BusRoute.scala
```

The program performs graph construction, centrality analysis, Pregel-based shortest paths, and route optimization.

### Step 6: Generate Visualizations

From the project root:

```bash
python viz/plot.py
```

The visualization scripts generate static network diagrams and an interactive Folium map.

---

## 🧠 Algorithms Used

| Algorithm / Technique | Purpose |
|---|---|
| Degree Centrality | Identifying important transit hubs |
| Haversine Formula | Calculating geographical distances |
| Pregel | Distributed iterative graph computation |
| Single Source Shortest Path | Finding minimum-distance paths |
| Vertex-Cut Partitioning | Distributing graph edges across partitions |

---

## ⚠️ Limitations

- Degree centrality is a connectivity-based congestion proxy, not actual passenger congestion.
- GTFS schedules do not provide real-time road traffic conditions.
- The 1.3 road winding factor is an approximation.
- The proposed express corridor is a simulation and does not represent an implemented public bus route.
- Performance results are based on local multi-core execution, not a production multi-machine cluster.

---

## 🔮 Future Improvements

- Integrate real-time traffic information.
- Introduce PySpark-based data engineering pipelines.
- Add passenger demand prediction using Machine Learning.
- Incorporate actual bus GPS tracking data.
- Develop dynamic route recommendations.
- Deploy the processing pipeline on a distributed Spark cluster.
- Build an interactive web dashboard for route analytics.

---

## 👨‍💻 Author

**Aman Singh**

Course: Cluster Computing (INT315)

Institution: Lovely Professional University

---

## 📜 License

This project is intended for academic and research purposes. Refer to the original dataset provider for applicable data licensing and usage terms.

---

⭐ If you find this project interesting, consider giving the repository a star!
