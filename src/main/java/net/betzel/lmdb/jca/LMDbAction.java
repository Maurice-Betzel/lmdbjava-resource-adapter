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
package net.betzel.lmdb.jca;

import org.lmdbjava.LmdbException;

/**
 * Created by mbetzel on 20.04.2017.
 */
public enum LMDbAction {
    DELETE_KEY(0), DELETE_KEY_VALUE(1), PUT(2), DROP(3);

    private int action;

    LMDbAction(final int action) {
        this.action = action;
    }

    public static LMDbAction parseLMDbAction(int action) {
        switch (action) {
            case 0:
                return LMDbAction.DELETE_KEY;
            case 1:
                return LMDbAction.DELETE_KEY_VALUE;
            case 2:
                return LMDbAction.PUT;
            case 3:
                return LMDbAction.DROP;
            default:
                throw new LmdbException("Unknown database action");
        }
    }

    public int getAction() {
        return action;
    }

}