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