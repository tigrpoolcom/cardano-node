{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -freduction-depth=0 #-}

import Cardano.Api
    ( Block(..),
      BlockHeader(BlockHeader),
      ChainPoint,
      ChainTip(ChainTip),
      NetworkId(Mainnet),
      BlockNo(BlockNo) )
import qualified Cardano.Api.IPC as IPC
import qualified Cardano.Chain.Slotting as Byron (EpochSlots(..))

-- TODO: Export this via Cardano.Api
import Ouroboros.Network.Protocol.ChainSync.ClientPipelined
    ( ChainSyncClientPipelined(ChainSyncClientPipelined),
      ClientPipelinedStIdle(SendMsgDone, SendMsgRequestNextPipelined,
                            CollectResponse),
      ClientStNext(..) )
import Network.TypedProtocol.Pipelined
    ( N(..), Nat(..), natToInt, unsafeIntToNat )

import Control.Monad (when)
import Data.Kind
import Data.Proxy
import Data.Time
import qualified GHC.TypeLits as GHC
import System.Environment (getArgs)
import System.FilePath ((</>))

-- | Connects to a local cardano node, requests the blocks and prints out the
-- number of transactions. To run this, you must first start a local node e.g.:
--
--     $ cabal run cardano-node:cardano-node -- run \
--        --config        configuration/cardano/mainnet-config.json \
--        --topology      configuration/cardano/mainnet-topology.json \
--        --database-path db \
--        --socket-path   db/node.sock \
--        --host-addr     127.0.0.1 \
--        --port          3001 \
--
-- Then run this with the path to the directory containing node.sock:
--
--     $ cabal run cardano-client-demo-pipeline -- db
--
main :: IO ()
main = do
  -- Get cocket path from CLI argument.
  socketDir:_ <- getArgs
  let socketPath = socketDir </> "node.sock"

  -- Connect to the node.
  putStrLn $ "Connecting to socket: " <> socketPath
  IPC.connectToLocalNode
    (connectInfo socketPath)
    protocols
  where
  connectInfo :: FilePath -> IPC.LocalNodeConnectInfo IPC.CardanoMode
  connectInfo socketPath =
      IPC.LocalNodeConnectInfo {
        IPC.localConsensusModeParams = IPC.CardanoModeParams (Byron.EpochSlots 21600),
        IPC.localNodeNetworkId       = Mainnet,
        IPC.localNodeSocketPath      = socketPath
      }

  protocols :: IPC.LocalNodeClientProtocolsInMode IPC.CardanoMode
  protocols =
      IPC.LocalNodeClientProtocols {
        IPC.localChainSyncClient    = Just (Left (chainSyncClient (safeIntToNat (Proxy @50)))),
        IPC.localTxSubmissionClient = Nothing,
        IPC.localStateQueryClient   = Nothing
      }

-- What's the correct way to do this?
safeIntToNat :: GHC.KnownNat ghcn => Proxy ghcn -> Nat (ToNat ghcn)
safeIntToNat p = unsafeIntToNat (fromInteger $ GHC.natVal p)
type family ToNat (n :: GHC.Nat) :: N where
  ToNat 0 = Z
  ToNat n = S (ToNat (n GHC.- 1))


-- | Defines the pipelined client side of the chain sync protocol.
chainSyncClient
  :: Nat (S n)
  -- ^ The maximum number of concurrent requests.
  -> ChainSyncClientPipelined
        (IPC.BlockInMode IPC.CardanoMode)
        ChainPoint
        ChainTip
        IO
        ()
chainSyncClient pipelineSize = ChainSyncClientPipelined $ do
  startTime <- getCurrentTime
  let
    clientIdle_RequestMoreN :: Nat n -> ClientPipelinedStIdle n (IPC.BlockInMode IPC.CardanoMode)
                                 ChainPoint ChainTip IO ()
    clientIdle_RequestMoreN n = case n of
      Succ predN | natToInt n >= natToInt pipelineSize -> CollectResponse Nothing (clientNextN predN)
      _ -> SendMsgRequestNextPipelined (clientIdle_RequestMoreN (Succ n))

    clientNextN :: Nat n -> ClientStNext n (IPC.BlockInMode IPC.CardanoMode)
                                 ChainPoint ChainTip IO ()
    clientNextN n =
      ClientStNext {
          recvMsgRollForward = \(IPC.BlockInMode block@(Block (BlockHeader _ _ currBlockNo@(BlockNo blockNo)) _) _) tip -> do
            when (blockNo `mod` 1000 == 0) $ do
              printBlock block
              now <- getCurrentTime
              let elapsedTime = realToFrac (now `diffUTCTime` startTime) :: Double
                  rate = fromIntegral blockNo / elapsedTime
              putStrLn $ "Rate = " ++ show rate ++ " blocks/second"
            case tip of
              ChainTip _ _ tipBlockNo | tipBlockNo == currBlockNo
                -> clientIdle_DoneN n
              _ -> return (clientIdle_RequestMoreN n)
        , recvMsgRollBackward = \_ _ -> putStrLn "Rollback" >> return (clientIdle_RequestMoreN n)
        }

    clientIdle_DoneN :: Nat n -> IO (ClientPipelinedStIdle n (IPC.BlockInMode IPC.CardanoMode)
                                 ChainPoint ChainTip IO ())
    clientIdle_DoneN n = case n of
      Succ predN -> do
        putStrLn "Chain Sync: done! (Ignoring remaining responses)"
        return $ CollectResponse Nothing (clientNext_DoneN predN) -- Ignore remaining message responses
      Zero -> do
        putStrLn "Chain Sync: done!"
        return $ SendMsgDone ()

    clientNext_DoneN :: Nat n -> ClientStNext n (IPC.BlockInMode IPC.CardanoMode)
                                 ChainPoint ChainTip IO ()
    clientNext_DoneN n =
      ClientStNext {
          recvMsgRollForward = \_ _ -> clientIdle_DoneN n
        , recvMsgRollBackward = \_ _ -> clientIdle_DoneN n
        }

    printBlock :: Block era -> IO ()
    printBlock (Block (BlockHeader _ _ currBlockNo) transactions)
      = putStrLn $ show currBlockNo ++ " transactions: " ++ show (length transactions)

  return (clientIdle_RequestMoreN Zero)