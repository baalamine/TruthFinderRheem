/**
 * 
 */
package org.qcri.truthdiscovery.scalable.truthfinder.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.qcri.rheem.core.data.KeyValuePair;
import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.truthdiscovery.logical.Compute;
import org.qcri.truthdiscovery.util.dataModel.data.Globals;
import org.qcri.truthdiscovery.util.dataModel.data.Source;
import org.qcri.truthdiscovery.util.dataModel.data.SourceClaim;
import org.qcri.truthdiscovery.util.dataModel.data.ValueBucket;
import org.qcri.truthdiscovery.util.dataModel.dataFormatter.DataComparator;

/**
 * @author mlba
 * @created date: Jan 25, 2016
 * Scalable Truth Finder Compute
 * 
 * @Input: List of buckets, each bucket consisting of 
 * a collection of claims about the same property of 
 * the object
 * @Output: a bucket with an updated confidence value
 */
public class ConfidenceComputation extends Compute<KeyValuePair<Integer, KeyValuePair<Integer, List<ValueBucket>>>, KeyValuePair<Integer, KeyValuePair<Integer, List<SourceClaim>>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4480059771907971424L;
	List<ValueBucket> bucketList = new ArrayList<ValueBucket>() ;
	@SuppressWarnings("unchecked")
	@Override
	public KeyValuePair<Integer, KeyValuePair<Integer, List<ValueBucket>>> compute(KeyValuePair<Integer, KeyValuePair<Integer, List<SourceClaim>>> input, RheemContext context) 
	{
		// TODO Auto-generated method stub
		System.out.println("Compute Transform") ;
		ArrayList<Double> params      = (ArrayList<Double>) context.getByKey("params") ;
 		double similarityConstant     = params.get(0) ;
		double base_sim 		          = params.get(1) ;
		double damping_factor         = params.get(2) ;
		
		org.qcri.truthdiscovery.util.dataModel.data.DataSet dd = new org.qcri.truthdiscovery.util.dataModel.data.DataSet() ;
		Integer k = input.value.key ; // data item key
		List<SourceClaim> kv = new ArrayList<SourceClaim>(input.value.value) ; // collection of claims
		
		for (SourceClaim c : kv) 
				dd.addClaim(c.getId(), c.getObjectIdentifier(), "", c.getPropertyName(), c.getPropertyValueString(), 1.0, c.getTimeStamp(), c.getSource().getSourceIdentifier()) ;
		
		dd.computeValueBuckets(false);
	  if ( dd.getDataItemsBuckets().size() == 0 )  return new KeyValuePair<Integer, KeyValuePair<Integer, List<ValueBucket>>>(1, new KeyValuePair<>(k, null)) ;
		
		/**
		 * retrieve new source trustworthiness scores
		 */
		ArrayList<?> trustworthiness = ((ArrayList<?>)context.getByKey("trustworthiness")) ;
		if ( ! trustworthiness.isEmpty() )
		{
			
			int i = trustworthiness.size() ;
			if ( trustworthiness.get(i-1) instanceof List)
			{
				//System.out.println("Spark") ;
				ArrayList<String> scores = (ArrayList<String>) trustworthiness.get(i - 1) ; 
				for (String s1 : scores)
				{
					String sourceIdentifier          = s1.split("==>")[0] ;
					double sourceTrustworthiness     = Double.parseDouble(s1.split("==>")[1]) ;
					if (dd.getSourcesHash().keySet().contains(sourceIdentifier)) 
						dd.getSourcesHash().get(sourceIdentifier).setTrustworthiness(sourceTrustworthiness) ; 
				}
			}
			else
			{ 
				//System.out.println("Java") ;
				ArrayList<String> scores = (ArrayList<String>) trustworthiness ;
				for (String s1 : scores )
				{
					String sourceIdentifier          = s1.split("==>")[0] ;
					double sourceTrustworthiness     = Double.parseDouble(s1.split("==>")[1]) ;
					if (dd.getSourcesHash().keySet().contains(sourceIdentifier)) 
						dd.getSourcesHash().get(sourceIdentifier).setTrustworthiness(sourceTrustworthiness) ; 
				}
			}
		}
		
		bucketList      = dd.getDataItemsBuckets().values().iterator().next() ;
		this.computeConfidenceScore(bucketList) ;
		this.computeConfidenceWithSimilarity(bucketList, similarityConstant, base_sim) ;
		this.computeConfidence(bucketList, damping_factor) ;
		
		/*
		List<SourceClaim> b = bucketList.stream()
				.map( d -> d.getClaims() )
				.flatMap( l -> l.stream() )
				.collect(Collectors.toList()) ;
		
		for (SourceClaim claim : b)
			System.out.println(claim.getId() + " <==> " + claim.getSource().getSourceIdentifier() ) ;
		*/
		return new KeyValuePair<Integer, KeyValuePair<Integer, List<ValueBucket>>>(1, new KeyValuePair<>(k, bucketList)) ;
	}
	/**
	 * Confidence without similarity values
	 * @param bucketsList
	 * @return@@
	 */
	public List<ValueBucket> computeConfidenceScore(List<ValueBucket> bucketsList)
	{
		double ln    = 0 ;
		double lnSum = 0 ;
		
		/* 
		 * All claims, from different sources, for single property value.
		 */
		for (ValueBucket bucket : bucketsList) 
		{
			 lnSum = 0;
			for (Source source : bucket.getSources()) 
			{
					ln    = Math.log(1 - source.getTrustworthiness());
					lnSum = lnSum - ln;
				
			}
			bucket.setConfidence(lnSum);
		}
		
		return bucketsList ;
		
	}
	/**
	 * Confidence with similarity values
	 * @param bucketsList
	 * @param similarityConstant
	 * @param base_sim
	 * @return List<ValueBucket>
	 */
	public List<ValueBucket> computeConfidenceWithSimilarity(List<ValueBucket> bucketsList,  double similarityConstant, double base_sim)
	{
		double similarity 	 ;
		double similaritySum ;
		
		for (ValueBucket bucket1 : bucketsList)
		{
			similaritySum = 0 ;
			for (ValueBucket bucket2 : bucketsList)
			{
				// test if same bucket and continue
				if (bucket1.getClaims().get(0).getId() == bucket2.getClaims().get(0).getId()) {
					continue;
				}
				similarity = computeClaimsSimilarity(bucket1, bucket2, base_sim);
				similaritySum = similaritySum + (bucket2.getConfidence() * similarity);
			}
			/*
			 * compute the similarity based on the confidence without similarity
			 */
			similaritySum = (similarityConstant * similaritySum) + bucket1.getConfidence();
			bucket1.setConfidenceWithSimilarity(similaritySum);
		}
		/*
		 * populate the confidence computed with similarity to be the exact new value for 
		 * the claim confidence
		 */
		for (ValueBucket bucket : bucketsList) {
			bucket.setConfidence(bucket.getConfidenceWithSimilarity());
		}
		
		return bucketsList;
		
	}
	/**
	 * Final Confidence computation
	 * @param bucketsList
	 * @param dampingFactor
	 * @return List<ValueBucket>
	 */
	public List<ValueBucket> computeConfidence(List<ValueBucket> bucketsList, double dampingFactor)
	{
		double e, denum ;
		for (ValueBucket b : bucketsList)
		{
			e = -1 * dampingFactor * b.getConfidence();
			e = Math.exp(e);
			denum = 1 + e;
			b.setConfidence(1/denum);
		}
		
		return bucketsList;	
	}
	/**
	 * Similarity computation
	 * @param bucket1
	 * @param bucket2
	 * @param base_sim
	 * @return double
	 */
	private double computeClaimsSimilarity(ValueBucket bucket1, ValueBucket bucket2, double base_sim) 
	{
		// TODO Auto-generated method stub
		double result = DataComparator.computeImplication(bucket1, bucket2, bucket1.getValueType());
		result = result - base_sim;
		if (Double.isNaN(result)) {
			return 1;
		}
		// TODO : revise for the non-string values  
		return (double)result;
	}
	

}
