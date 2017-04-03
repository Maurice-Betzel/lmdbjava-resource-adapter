package net.betzel.lmdb.jca;

import javax.resource.spi.ConnectionRequestInfo;

/**
 * Created by mbetzel on 03.04.2017.
 */
public interface LMDbConnectionRequestInfo extends ConnectionRequestInfo {

    public String getDatabaseName();

}