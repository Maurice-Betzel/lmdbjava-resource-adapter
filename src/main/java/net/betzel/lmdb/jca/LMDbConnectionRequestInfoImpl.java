package net.betzel.lmdb.jca;

/**
 * Created by mbetzel on 03.04.2017.
 */
public class LMDbConnectionRequestInfoImpl implements LMDbConnectionRequestInfo {

    private final String databaseName;

    public LMDbConnectionRequestInfoImpl(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LMDbConnectionRequestInfoImpl that = (LMDbConnectionRequestInfoImpl) o;

        return databaseName.equals(that.databaseName);
    }

    @Override
    public int hashCode() {
        return databaseName.hashCode();
    }

}