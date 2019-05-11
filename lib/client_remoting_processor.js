'use strict';

const ByteBuffer = require('byte');
const RequestCode = require('./protocol/request_code');
const MessageConst = require('./message/message_const');
const MessageDecoder = require('./message/message_decoder');

class ClientRemotingProcessor {
  constructor(mqClient) {
    this._mqClient = mqClient;
    this._mqClient.on('request', (entity, address) => {
      if (entity.data && entity.data.type === 'REQUEST_COMMAND') {
        this.processRequest(entity.data, address);
      }
    });
  }

  processRequest(data, address) {
    switch (data.code) {
      case RequestCode.CHECK_TRANSACTION_STATE: {
        return this.checkTransactionState(data, address);
      }
      // TODO: process ALL kinds of processRequest from broker
      // case RequestCode.RESET_CONSUMER_CLIENT_OFFSET: {
      // }
      default:
        break;
    }
  }

  checkTransactionState(request, address) {
    const requestHeader = request.decodeCommandCustomHeader();
    const byteBuffer = ByteBuffer.wrap(request.body);
    const messageExt = MessageDecoder.decode(byteBuffer);
    if (messageExt) {
      const transactionId = messageExt.properties[MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX];
      if (transactionId) {
        messageExt.transactionId = transactionId;
      }
      const group = messageExt.properties[MessageConst.PROPERTY_PRODUCER_GROUP];
      if (group) {
        const producer = this._mqClient.selectProducer(group);
        if (producer) {
          producer.checkTransactionState(address, messageExt, requestHeader);
        } else {
          this._mqClient.logger.debug('checkTransactionState, pick producer by group[{%s}] failed', group);
        }
      } else {
        this._mqClient.logger.warn('checkTransactionState, pick producer group failed');
      }
    } else {
      this._mqClient.logger.warn('checkTransactionState, decode message failed');
    }

    return null;
  }
}

module.exports = ClientRemotingProcessor;
