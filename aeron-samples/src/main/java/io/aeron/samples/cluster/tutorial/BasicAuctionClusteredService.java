/*
 * Copyright 2014-2023 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples.cluster.tutorial;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.*;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.IdleStrategy;

import java.util.Objects;
import java.time.*; 

/**
 * Auction service implementing the business logic.
 */
// tag::new_service[]
public class BasicAuctionClusteredService implements ClusteredService
// end::new_service[]
{
    public static final int CORRELATION_ID_OFFSET = 0;
    public static final int CUSTOMER_ID_OFFSET = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    public static final int LARGE_EVEN_OFFSET = CUSTOMER_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    public static final int LARGE_ODD_OFFSET = LARGE_EVEN_OFFSET + BitUtil.SIZE_OF_LONG;
    public static final int BID_MESSAGE_LENGTH = LARGE_ODD_OFFSET + BitUtil.SIZE_OF_LONG;
    public static final int BID_SUCCEEDED_OFFSET = BID_MESSAGE_LENGTH;
    public static final int EGRESS_MESSAGE_LENGTH = BID_SUCCEEDED_OFFSET + BitUtil.SIZE_OF_BYTE;

    public static final int SNAPSHOT_CUSTOMER_ID_OFFSET = 0;
    // NOTE TAHA : Ensure it is done harmoniously wherever used
    // THE ONLY STATEFUL THING WE ARE STORING IS THE LARGEST EVEN AND LARGEST ODD
    // THAT THEREFORE NEEDS TO BE RETAINED AND RELFECTED IN THE SNAPSHOTTING AND
    // CLUSTERING

    public static final int SNAPSHOT_EVEN_OFFSET = SNAPSHOT_CUSTOMER_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    public static final int SNAPSHOT_ODD_OFFSET = SNAPSHOT_EVEN_OFFSET + BitUtil.SIZE_OF_LONG;
    public static final int SNAPSHOT_MESSAGE_LENGTH = SNAPSHOT_ODD_OFFSET + BitUtil.SIZE_OF_LONG;

    private final MutableDirectBuffer egressMessageBuffer = new ExpandableArrayBuffer();
    private final MutableDirectBuffer snapshotBuffer = new ExpandableArrayBuffer();

    // tag::state[]
    private final Auction auction = new Auction();
    // end::state[]
    private Cluster cluster;
    private IdleStrategy idleStrategy;

    /**
     * {@inheritDoc}
     */
    // tag::start[]
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster; // <1>
        this.idleStrategy = cluster.idleStrategy(); // <2>

        if (null != snapshotImage) // <3>
        {
            loadSnapshot(cluster, snapshotImage);
        }
    }
    // end::start[]

    /**
     * {@inheritDoc}
     */
    // tag::message[]
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        final long correlationId = buffer.getLong(offset + CORRELATION_ID_OFFSET); // <1>
        final long customerId = buffer.getLong(offset + CUSTOMER_ID_OFFSET);
        final long LargeEven = buffer.getLong(offset + LARGE_EVEN_OFFSET);
        final long largeOdd = buffer.getLong(offset + LARGE_ODD_OFFSET);
        /*
         * NOTE TAHA: THE CLIENT WILL BE SENDING THE FOLLOWING:
         * CORRELATIONID ( AS IS LOGIC)
         * CUSTOMERID ( AS IS LOGIC)
         * WHATEVER LARGE EVEN ITS USER TYPED
         * WHATEVER LARGE ODD ITS USER TYPED
         */

        // final long price = buffer.getLong(offset + PRICE_OFFSET);

        boolean bidSucceeded = auction.attemptBid(LargeEven, largeOdd); // <2>
        /*
         * NOTE TAHA
         * calling this function , having a simple comparison logic
         * but retaining the variable name as bidSucceeded ( DIRTY QUICK HACK )
         * 
         * THE FUNCTION ITSELF HAS BEEN CHANGED
         * 
         */

        if (null != session) // <3>
        {
            egressMessageBuffer.putLong(CORRELATION_ID_OFFSET, correlationId); // <4>
            // egressMessageBuffer.putLong(CUSTOMER_ID_OFFSET,
            // auction.getCurrentWinningCustomerId());
            // egressMessageBuffer.putLong(PRICE_OFFSET, auction.getBestPrice());
            egressMessageBuffer.putByte(BID_SUCCEEDED_OFFSET, bidSucceeded ? (byte) 1 : (byte) 0);


            // note TAHA : If this is in Matcher , then call ingress of Hedgder
            // if this is in Hedger , call ingress of Risker
//
//            switch ( customerId)
//            {
//                case 9 : //this will be sent from cluster client to the Matcher
//                System.out.println( " OnSessionMessage -> Got magic number 9 , acting as MATCHER");
//
//                //cluster
//                break;
//
//                case 8: //this will be sent from  Matcher to the hedger
//                System.out.println( " OnSessionMessage -> Got magic number 9 , acting as HEDGER");
//                break;
//
//                case 7: //this will be sent from Hedger to Risker
//                System.out.println( " OnSessionMessage -> Got magic number 9 , acting as RISKER");
//                break;
//
//
//
//            }

            idleStrategy.reset();
            while (session.offer(egressMessageBuffer, 0, EGRESS_MESSAGE_LENGTH) < 0) // <5>
            {
                idleStrategy.idle(); // <6>
            }
        }
    }
    // end::message[]

    /**
     * {@inheritDoc}
     */
    // tag::takeSnapshot[]
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        // snapshotBuffer.putLong(SNAPSHOT_CUSTOMER_ID_OFFSET,
        // auction.getCurrentWinningCustomerId()); // <1>
        snapshotBuffer.putLong(SNAPSHOT_EVEN_OFFSET, auction.getHighestEven() /* get from local object state */);
        snapshotBuffer.putLong(SNAPSHOT_ODD_OFFSET, auction.getHighestOdd() /* get from local object state */);
        /*
         * TAHA TODO !!
         * check how to call above getters from the auction object which is
         * actually retaining the required state after applying
         * the "business logic" of the comparisons
         * 
         */

        idleStrategy.reset();
        while (snapshotPublication.offer(snapshotBuffer, 0, SNAPSHOT_MESSAGE_LENGTH) < 0) // <2>
        {
            idleStrategy.idle();
        }
    }
    // end::takeSnapshot[]

    // tag::loadSnapshot[]
    private void loadSnapshot(final Cluster cluster, final Image snapshotImage) {
        final MutableBoolean isAllDataLoaded = new MutableBoolean(false);
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> // <1>
        {
            assert length >= SNAPSHOT_MESSAGE_LENGTH; // <2>

            final long largeEven = buffer.getLong(offset + SNAPSHOT_EVEN_OFFSET);
            final long largeOdd = buffer.getLong(offset + SNAPSHOT_ODD_OFFSET);

            // change this TODO , done !
            auction.loadInitialState(largeEven, largeOdd); // <3>

            isAllDataLoaded.set(true);
        };

        while (!snapshotImage.isEndOfStream()) // <4>
        {
            final int fragmentsPolled = snapshotImage.poll(fragmentHandler, 1);

            if (isAllDataLoaded.value) // <5>
            {
                break;
            }

            idleStrategy.idle(fragmentsPolled); // <6>
        }

        assert snapshotImage.isEndOfStream(); // <7>
        assert isAllDataLoaded.value;
    }
    // end::loadSnapshot[]

    /**
     * {@inheritDoc}
     */
    public void onRoleChange(final Cluster.Role newRole) {
    }

    /**
     * {@inheritDoc}
     */
    public void onTerminate(final Cluster cluster) {
    }

    /**
     * {@inheritDoc}
     */
    public void onSessionOpen(final ClientSession session, final long timestamp) {
        System.out.println("onSessionOpen(" + session + ")");
    }

    /**
     * {@inheritDoc}
     */
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
        System.out.println("onSessionClose(" + session + ")");
    }

    /**
     * {@inheritDoc}
     */
    public void onTimerEvent(final long correlationId, final long timestamp) {
    }

    static class Auction {
        /*
         * NOTE TAHA
         * THIS CLASS IS NO LONGER NEEDED BUT KEEPING THE CODE SKELETAL
         * STRUCTURE AND DOING QUICK AND DIRTY HACK
         * 
         */
        private long bestPrice = 0;
        private long currentWinningCustomerId = -1;

        // TAHA : ADDING NEW DATA MEMBERS TO RETAIN REQUIRED STATE
        // initialising to a low "high" of Zero for both
        private long currentLargestEven = 0;
        private long currentLargestOdd = 0;

        void loadInitialState(final long highEven, final long highOdd) {
            currentLargestEven = highEven;
            currentLargestOdd = highOdd;
        }

        // NOTE TAHA : DOING A QUICK AND DIRTY HACK HERE
        boolean attemptBid(final long highEven, final long highOdd) {
            Instant rightNow = Instant.now();
            
            System.out.println("TimeInstant:" + rightNow + "currentLargestEven=" + this.currentLargestEven + "currentLargestOdd=" + this.currentLargestOdd +", highEven=" + highEven + ",highOdd=" + highOdd + ")");

            // if (price <= bestPrice)

            // NOTE TAHA : the lines below are the "BUSINESS LOGIC"

            if (highEven > currentLargestEven) {
                currentLargestEven = highEven;
            }
            if (highOdd > currentLargestOdd) {
                currentLargestOdd = highOdd;
            }
            if (highEven < currentLargestEven || highOdd < currentLargestOdd) {
                /*
                 * NOTE TAHA : Client wins ONLY if BOTH his numbers are larger than existing
                 * numbers
                 * although if only one of them is , we will store it anyway :)
                 */
                return false;
            }

            // NOTE TAHA not needed currentWinningCustomerId = customerId;

            return true;
        }

        /*
         * NOTE TAHA
         * Two new helper getter functions are needed , hence adding
         */

        long getHighestEven() {
            return currentLargestEven;
        }

        long getHighestOdd() {
            return currentLargestOdd;
        }

        long getBestPrice() {
            return bestPrice;
        }

        long getCurrentWinningCustomerId() {
            // NOTE TAHA: THIS FUNCTION IS NOT NEEDED ANYMORE , JUST LEAVING IT THOUGH
            return currentWinningCustomerId;
        }

        /**
         * {@inheritDoc}
         */
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Auction auction = (Auction) o;

            /*
             * NOTE TAHA: THE LOGIC BELOW IS WRONG AND NEEDS TO BE CHANGED
             */

            return ((currentLargestEven == auction.currentLargestEven) &&
                    (currentLargestOdd == auction.currentLargestOdd));

            // NOTE TAHA : CHANGING THE LOGIC , COMMENTING OUT OLD LOGIC
            // return bestPrice == auction.bestPrice && currentWinningCustomerId ==
            // auction.currentWinningCustomerId;
        }

        /**
         * {@inheritDoc}
         */
        public int hashCode() {
            // NOTE TAHA CHANGING THE HASH TO REFLECT BOTH NUMBERS
            // return Objects.hash(bestPrice, currentWinningCustomerId);
            return Objects.hash(currentLargestEven, currentLargestOdd);
        }

        /**
         * {@inheritDoc}
         */
        public String toString() {
            return "Auction{" +
                    "currentLargestEven=" + currentLargestEven +
                    ", currentLargestOdd=" + currentLargestOdd +
                    '}';
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final BasicAuctionClusteredService that = (BasicAuctionClusteredService) o;

        return auction.equals(that.auction);
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode() {
        return Objects.hash(auction);
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        return "BasicAuctionClusteredService{" +
                "auction=" + auction +
                '}';
    }
}

