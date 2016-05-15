/**
 * 
 */
package org.qcri.truthdiscovery.scalable.truthfinder.logical;

import java.util.ArrayList;
import org.qcri.rheem.core.logicallayer.GlobalVariableMerger;

import gnu.trove.map.hash.THashMap;

/**
 * @author mlba
 * Scalable Truth Finder Merger
 * Updating global shared variables
 */
public class ScalableTruthFinderMerger extends GlobalVariableMerger {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public THashMap<String, Object> merge(THashMap<String, Object> hashMap) 
	{
		System.out.println("Merger Operator") ;
		THashMap<String, Object> newHashMap = new THashMap<>();
		@SuppressWarnings("unchecked")
		ArrayList<String> updatedSources = (ArrayList<String>) hashMap.get("updatedSources") ;
		newHashMap.put("trustworthiness", updatedSources);
	  return newHashMap;
	}

}
