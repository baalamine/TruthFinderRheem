/**
 * 
 */
package org.qcri.truthdiscovery.fullyscalable.truthfinder.logical;


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
public class TrustworthinessUpdate extends Update<Double, KeyValuePair<Integer, KeyValuePair<Source, List<SourceClaim>>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6071548655967414404L;
	

	@SuppressWarnings("rawtypes")
	@Override
	public Double update(KeyValuePair<Integer, KeyValuePair<Source, List<SourceClaim>>> input, RheemContext context) 
	{
		// TODO Auto-generated method stub
		System.out.println("Truthworthiness Update Operator") ;
		
	
		// Update source trustworthiness scores
		@SuppressWarnings("unchecked")
		ArrayList<Double> params = (ArrayList<Double>) context.getByKey("params") ;
		double dampingFactor = params.get(2) ;
		Integer sourceIdentifier = input.key ;
	  KeyValuePair<Source, List<SourceClaim>> data = input.value ;
	  //System.out.println("Number of total claims is : " + data.value.size()) ;
		
		/**
		 * TODO
		 * @lamine ==> this method has to be re-implemented 
		 * Input: <SourceId, List<SourceClaim>>
		 * Procedure: 
		 * 					1. Update source trustworthiness
		 * 					2. compute source trustworthiness oscillation
		 * 
		 */
		computeTrustworthiness(dampingFactor, data.key, data.value) ;
		ArrayList<String> scores = new ArrayList<String>( ) ; scores.add(sourceIdentifier + "==>" + data.key.getTrustworthiness()) ;
		context.push("UpdatedSources", scores) ;
		double cosineSimilarity = computeTrustworthinessCosineSimilarity(data.key) ;
		
		return cosineSimilarity;
	}
	

	/**
	 * Compute the cosine similarity between the trustworthiness computed in the current iteration 
	 * and the trustworthiness computed in the previous iteration. 
	 * @return the cosine similarity
	 */
	public static double computeTrustworthinessCosineSimilarity(Source source) {
		double a,b;
		double sumAB = 0;
		double sumA2=0;
		double sumB2 = 0;
		
		//for (Source s : listSources) {
			a = source.getOldTrustworthiness();
			b = source.getTrustworthiness();
			sumAB = sumAB + (a*b);
			sumA2 = sumA2 + (a*a);
			sumB2 = sumB2 + (b*b);
		//}
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
	private void computeTrustworthiness(double dampingFactor, Source source, List<SourceClaim> claims) {
	
	   double sum = 0;
			for (SourceClaim claim : claims)
			{
				sum = sum + (1 - Math.exp((-1 * dampingFactor) * claim.getBucket().getConfidence()));
			}
	
			source.setOldTrustworthiness(source.getTrustworthiness());
			source.setTrustworthiness(sum / source.getClaims().size());
		//	System.out.println(source.getSourceIdentifier());
		//  System.out.format("old trust:%f\t new trust:%f\n", source.getOldTrustworthiness(), source.getTrustworthiness()) ;
		}
	
	
}
