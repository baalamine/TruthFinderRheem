/**
 * 
 */
package org.qcri.truthdiscovery.scalable.truthfinder.logical;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.qcri.rheem.core.data.KeyValuePair;
import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.truthdiscovery.logical.Update;
import org.qcri.truthdiscovery.util.dataModel.data.Source;
import org.qcri.truthdiscovery.util.dataModel.data.SourceClaim;
import org.qcri.truthdiscovery.util.dataModel.data.ValueBucket;


/**
 * @author mlba
 * @created date: Jan 26, 2016
 * 
 * @Input: KeyValuePair<Integer, List<ValueBucket>>
 * @Output: Cosine similarity between source trustworthiness scores
 */
public class TrustworthinessUpdate extends Update<Double, KeyValuePair<Integer, KeyValuePair<Integer, List<ValueBucket>>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6071548655967414404L;
	

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Double update(KeyValuePair<Integer, KeyValuePair<Integer, List<ValueBucket>>> input, RheemContext context) 
	{
		// TODO Auto-generated method stub
		System.out.println("Update Transform") ;
		double cosine = 0 ;
		ArrayList<Double> params = (ArrayList<Double>) context.getByKey("params") ;
 		double dampingFactor = params.get(2) ;
	
 		//Trustworthiness computation
		ArrayList<ValueBucket> buckets = (ArrayList<ValueBucket>) input.value.value ;	
		ArrayList<Source> listSources = computeTrustworthiness(dampingFactor, buckets) ;
		
	 //Update trust global variable and push
	  ArrayList<String> updatedScores = new ArrayList<String>() ;
		for (Source source : listSources) updatedScores.add(source.getSourceIdentifier()+ "==>" + source.getTrustworthiness()) ;
	 	context.push("updatedSources", updatedScores) ;
	 	context.put("trustworthiness", updatedScores) ;
	 	
		//Cosine similarity computation
		cosine = computeTrustworthinessCosineSimilarity(listSources) ;
		
		return cosine ;
	}
	

	/**
	 * Compute the cosine similarity between the trustworthiness computed in the current iteration 
	 * and the trustworthiness computed in the previous iteration. 
	 * @return the cosine similarity
	 */
	public static double computeTrustworthinessCosineSimilarity(List<Source> listSources) {
		double a,b;
		double sumAB = 0;
		double sumA2=0;
		double sumB2 = 0;
		
		for (Source s : listSources) {
			a = s.getOldTrustworthiness();
			b = s.getTrustworthiness();
			sumAB = sumAB + (a*b);
			sumA2 = sumA2 + (a*a);
			sumB2 = sumB2 + (b*b);
		}
		sumA2 = Math.pow(sumA2, 0.5);
		sumB2 = Math.pow(sumB2, 0.5);
		if ((sumA2 * sumB2) == 0) {
			return Double.MAX_VALUE;
		}
		if (Double.isInfinite(sumAB)) {
			if (Double.isInfinite((sumA2 * sumB2))) {
				return 1.0;
			}
		}
		double cosineSimilarity = sumAB / (sumA2 * sumB2);
		return cosineSimilarity;
	}
	
	/**
	 * Update every source trustworthiness.
	 * The method doesn't delete the old trustworthiness value in order to be able to compute 
	 * the cosine similarity between the old and new trustworthiness values.
	 * it rather save it in the "oldTrustworthiness" property in the Source object.
	 */
	private ArrayList<Source> computeTrustworthiness(double dampingFactor, List<ValueBucket> buckets)
	{
		HashMap<Source, ArrayList<Double>> h = new HashMap<Source, ArrayList<Double>>() ;
		// Get confidence values for each sources
		for (ValueBucket bucketList: buckets)
		{
			
			if (bucketList == null) continue ;
			
			for (Source source : bucketList.getSources())
			{
				if ( ! h.keySet().contains(source) ) h.put(source, new ArrayList<Double>()) ;
				for (SourceClaim claim : source.getClaims()) {
						double vote = (1 - Math.exp((-1 * dampingFactor) * claim.getBucket().getConfidence()));
						h.get(source).add(vote) ;
					}
				
			}
			
		}
		
		for(Source source : h.keySet())
		{
			double sum = 0 ;
			double k = h.get(source).size() ;
			for (double vote : h.get(source))
			{
				sum = sum + vote ;
			}
			source.setOldTrustworthiness(source.getTrustworthiness());
			source.setTrustworthiness(sum / k);
			/*
			if (source.getSourceIdentifier().equalsIgnoreCase("1252049: MKil")) 
			{
					System.out.println(source.getSourceIdentifier()) ;
					System.out.format("old trust:%f\t new trust:%f\n", source.getOldTrustworthiness(), source.getTrustworthiness()) ;
			//		System.out.println("List of claims ==> " + h.get(source).size()) ;
				//	for (SourceClaim claim : source.getClaims())
					//	System.out.println(claim.getBucket().getId() + " ::: " +claim.getBucket().getConfidence()) ;
			}
				*/
			}
		
   return new ArrayList<Source>(h.keySet()) ;
		
	}
}
