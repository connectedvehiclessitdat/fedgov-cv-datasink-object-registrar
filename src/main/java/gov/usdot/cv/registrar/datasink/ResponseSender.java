package gov.usdot.cv.registrar.datasink;

import gov.usdot.asn1.generated.j2735.J2735;
import gov.usdot.asn1.generated.j2735.semi.DataConfirmation;
import gov.usdot.asn1.generated.j2735.semi.SemiDialogID;
import gov.usdot.asn1.generated.j2735.semi.SemiSequenceID;
import gov.usdot.asn1.generated.j2735.semi.Sha256Hash;
import gov.usdot.cv.common.asn1.GroupIDHelper;
import gov.usdot.cv.common.asn1.TemporaryIDHelper;
import gov.usdot.cv.common.inet.InetPacketSender;
import gov.usdot.cv.common.inet.InetPoint;
import gov.usdot.cv.security.SecurityHelper;
import gov.usdot.cv.security.crypto.CryptoProvider;

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.oss.asn1.Coder;
import com.oss.asn1.ControlTableNotFoundException;
import com.oss.asn1.InitializationException;

public class ResponseSender {

	private static final Logger logger = Logger.getLogger(ResponseSender.class);
	
	private CryptoProvider cryptoProvider = new CryptoProvider();
	private Coder coder;
	private InetPacketSender dataBundleSender;
	private MessageDigest messageDigest;
	
	public ResponseSender (String bundleForwarderHost, int bundleForwarderPort, boolean forwardAll) 
			throws ControlTableNotFoundException, InitializationException, NoSuchAlgorithmException {
		
		J2735.initialize();
		coder = J2735.getPERUnalignedCoder();
		SecurityHelper.initSecurity();
		messageDigest = MessageDigest.getInstance("SHA-256");
		
		InetPoint forwarderPoint = null;
		if (bundleForwarderHost != null && bundleForwarderPort != 0) {
			try {
				forwarderPoint = new InetPoint(InetAddress.getByName(bundleForwarderHost).getAddress(),
						bundleForwarderPort);
			} catch (UnknownHostException e) {
				logger.error("Error creating forwarder InetPoint ", e);
			}
		}
		
		logger.info(String.format("Forwarder host '%s' and port '%s'.", bundleForwarderHost, bundleForwarderPort));
		logger.info(String.format("Packet sender forwarding all response: %s", forwardAll));
		
		logger.info("Constructing result processors ...");
		dataBundleSender = new InetPacketSender(forwarderPoint);
		dataBundleSender.setForwardAll(forwardAll);
	}
	
	public void sendResponse(RegisterModel registerModel) throws Exception {
		
		DataConfirmation dc = buildDataConfirmation(registerModel);
		ByteArrayOutputStream sink = new ByteArrayOutputStream();
		coder.encode(dc, sink);
		byte [] payload = sink.toByteArray();
		
		byte[] certificate = registerModel.certificate != null ? Base64.decodeBase64(registerModel.certificate): null;
		if (certificate != null) {
			try {
				byte[] certID8 = SecurityHelper.registerCert(certificate, cryptoProvider);
				payload = SecurityHelper.encrypt(payload, certID8, cryptoProvider, SecurityHelper.DEFAULT_PSID);
			} catch (Exception ex) {
				logger.error("Couldn't encrypt outgoing message. Reason: " + ex.getMessage(), ex);
			}
		}
		
		int retries = 3;
		Exception lastEx = null;
		boolean sent = false;
		while (retries > 0) {
			try {
				logger.debug(String.format("Sending ObjectRegistrationDataConfirmation data for requestID %s", 
						registerModel.requestId));
				InetPoint destPoint = new InetPoint(registerModel.destHost, 
						registerModel.destPort, dataBundleSender.isForwardAll());
				dataBundleSender.forward(destPoint, payload, Boolean.valueOf(registerModel.fromForwarder));
				sent = true;
				break;
			} catch (Exception ex) {
				logger.error(String.format("Failed to send Object Discovery data for requestID %s", 
						registerModel.requestId));
				lastEx = ex;
			} finally {
				retries--;
			}
			
			try { Thread.sleep(10); } catch (InterruptedException ignore) {}
		}
		
		if (! sent && lastEx != null) throw lastEx;
		
		try { Thread.sleep(10); } catch (InterruptedException ignore) {}
	}
	
	private DataConfirmation buildDataConfirmation(RegisterModel registerModel) throws NoSuchAlgorithmException {
		DataConfirmation dc = new DataConfirmation();
		dc.setDialogID(SemiDialogID.objReg);
		dc.setSeqID(SemiSequenceID.dataConf);
		dc.setGroupID(GroupIDHelper.toGroupID(registerModel.groupId));
		dc.setRequestID(TemporaryIDHelper.toTemporaryID(registerModel.requestId));
		
		byte[] bytes = Base64.decodeBase64(registerModel.encodedMsg);
		byte[] hashBytes = messageDigest.digest(bytes);
		dc.setHash(new Sha256Hash(hashBytes));
		return dc;
	}
}