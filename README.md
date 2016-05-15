# Truth Finder on Rheem


Scalable Truth Finder using Rheem
The aims of this project is to provide a scalable version of TruthFinder algorithm, a truth discovery algorithm that harness correlations between similar claim values in order to improve the accuracy of a classical data fusion scenario where sources are overlapping on same object attributes, but they provide conflicting values.

Basically, we will rely on Rheem, an agile distributed data processing framework developed by the data analytic group at Qatar Computing Research Institute. Rheem provide three distinctive features: 1. it allows users to easily specify their jobs with easy-to-use interfaces, 2. it provides developers with opportunities to optimize performance in different ways, and 3. it can run on any execution platform, such as Spark or MapReduce and combinations of those.

## Truth Finder Rheem Operators

### ClaimReading procedure
This function reads a string of attribute values separated by tab and constructs the corresponding source claim object.

### ConfidenceComputation procedure
This function considers as input a bucket of source claims and compute the confidence score of each possible attribute value based on its sources' trustworthiness level.

### TrustworthinessUpdate procedure
This function considers source claims along with their confidence scores and updates the current trustworthiness score of each source by accounting for the newly computed confidence values.

### ConvergenceTesting procedure
This function takes as input the cosine similarity of the current and previous source trustworthiness scores and checks if a given user accuracy is reached. When such a accuracy is reached, the process ends.

## Truth Finder Rheem Versions
We provide two implemenations of truth finder on top of Rheem. 

### Semi-distributed version 
In this version, the TrustworthinessUpdate Function is not distributed. 


### Fully distributed version
In this version, all the different functions can be distributed. This version is not yet stable. 

## Usage

### Clone Github
First, clone the github repository in local containing the source codes of the two implementation versions. Through https, it is done by the following command: #### git clone https://github.com/baalamine/TruthFinderRheem.git

### Requirements and dependencies

To execute Truth Finder on top of Rheem, one has to have Java version 7 or later and maven installed. 
All the jar dependencies are defined in a pom file. Those dependencies need to be install to properly
run the application.

### Main classes

To execute and test Truth Finder on top of Rheem, you will need to run the following main class

#### ScalableTruthFinderMain.java

#### FullyScalableTruthFinderMaim.java 

In these main classes are specified the input parameters of the algorithm, in particular the claim dataset folder, the expected accuracy (or stopping criteria), and the initial trustworthiness score.

