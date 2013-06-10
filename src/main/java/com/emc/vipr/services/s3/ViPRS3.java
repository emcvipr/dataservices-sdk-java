/**
 * TODO: Copyright here
 */
package com.emc.vipr.services.s3;

import com.emc.vipr.services.s3.model.AppendObjectRequest;
import com.emc.vipr.services.s3.model.AppendObjectResult;
import com.emc.vipr.services.s3.model.UpdateObjectRequest;
import com.emc.vipr.services.s3.model.UpdateObjectResult;

/**
 * @author cwikj
 *
 */
public interface ViPRS3 {
	public UpdateObjectResult updateObject(UpdateObjectRequest request);
	
	public AppendObjectResult appendObject(AppendObjectRequest request);
}
