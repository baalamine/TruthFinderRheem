/**
 * 
 */
package org.qcri.truthdiscovery.fullyscalable.truthfinder.logical;

import java.util.ArrayList;
import org.qcri.rheem.core.logicallayer.GlobalVariableMerger;

import gnu.trove.map.hash.THashMap;

/**
 * @author mlba
 * Scalable Truth Finder Merger
 * Updating global shared variables
 */
public class FullyScalableTruthFinderMerger extends GlobalVariableMerger {

	@Override
	public THashMap<String, Object> merge(THashMap<String, Object> hashMap) 
	{
		// TODO Auto-generated method stub
		//System.out.println("Merger Operator") ;
		THashMap<String, Object> newHashMap = new THashMap<>();
		ArrayList<String> updatedSources = (ArrayList<String>) hashMap.get("updatedSources") ;
		newHashMap.put("trustworthiness", updatedSources);
	
    return newHashMap;
	}

}
