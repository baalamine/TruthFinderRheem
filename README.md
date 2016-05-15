# Truth Finder on Rheem


Scalable Truth Finder using Rheem
The aims of this project is to provide a scalable version of TruthFinder algorithm, a truth discovery algorithm that harness correlations between similar claim values in order to improve the accuracy of a classical data fusion scenario where sources are overlapping on same object attributes, but they provide conflicting values.

Basically, we will rely on Rheem, an agile distributed data processing framework developed by the data analytic group at Qatar Computing Research Institute. Rheem provide three distinctive features: 1. it allows users to easily specify their jobs with easy-to-use interfaces, 2. it provides developers with opportunities to optimize performance in different ways, and 3. it can run on any execution platform, such as Spark or MapReduce and combinations of those.

Trtuth Finder on Rheem defines the following set of operators to perform a tpyical truth discovery process, but in a distributed manner in order to scale in the presence of large datasets.

1. ClaimReading Function ==> read a string of attribute values separated by tab and constructs the corresponding source claim object
2. ConfidenceComputation Function ==> considers as input a bucket of source claims and compute the confidence score of each possible attribute value based on its sources' trustworthiness level
3. TrustworthinessUpdate Function ==> considers source claims along with their confidence scores and updates the current trustworthiness score of each source by accounting for the newly computed confidence values
4. ConvergenceTesting Function ==> this function takes as input the cosine similarity of the current and previous source trustworthiness scores and checks if a given user accuracy is reached. When such a accuracy is reached, the process ends.

This repository contains two implementation versions of Truth Finder on top of Rheem.

## Semi-distributed version 
In this version, the TrustworthinessUpdate Function is not distributed. 


## Fully distributed version
In this version, all the different functions can be distributed. This version is not yet stable. 

