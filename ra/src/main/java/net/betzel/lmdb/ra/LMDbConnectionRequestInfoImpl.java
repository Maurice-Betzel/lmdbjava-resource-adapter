/*
    Copyright 2017 Maurice Betzel

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package net.betzel.lmdb.ra;

/**
 * Created by mbetzel on 03.04.2017.
 */
public class LMDbConnectionRequestInfoImpl implements LMDbConnectionRequestInfo {

    private final String filePath;
    private final String databaseName;

    public LMDbConnectionRequestInfoImpl(String filePath, String databaseName) {
        this.filePath = filePath;
        this.databaseName = databaseName;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getFilePath() {
        return filePath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LMDbConnectionRequestInfoImpl that = (LMDbConnectionRequestInfoImpl) o;

        if (filePath != null ? !filePath.equals(that.filePath) : that.filePath != null) return false;
        return databaseName != null ? databaseName.equals(that.databaseName) : that.databaseName == null;
    }

    @Override
    public int hashCode() {
        int result = filePath != null ? filePath.hashCode() : 0;
        result = 31 * result + (databaseName != null ? databaseName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LMDbConnectionRequestInfoImpl{" +
                "filePath='" + filePath + '\'' +
                ", databaseName='" + databaseName + '\'' +
                '}';
    }

}