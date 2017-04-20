package net.betzel.lmdb.jca;

import org.lmdbjava.LmdbException;

/**
 * Created by mbetzel on 20.04.2017.
 */
public enum LMDbAction {
    DELETE(0), PUT(1), DROP(2);

    private int action;

    LMDbAction(final int action) {
        this.action = action;
    }

    public static LMDbAction parseLMDbAction(int action) {
        switch (action) {
            case 0: return LMDbAction.DELETE;
            case 1: return LMDbAction.PUT;
            case 2: return LMDbAction.DROP;
            default: throw new LmdbException("Unknown database action");
        }
    }

    public int getAction() {
        return action;
    }
}
