using AutoMapper;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using TJ.Operations.Common.Models.Requests;
using TJ.Operations.Common.Services;
using TJ.Operations.Api.Managers;
using TJ.Operations.Api.Models.Contexts;
using TJ.Operations.Api.Models.ViewModels;
using TJ.Operations.Api.Repositories;
using TJ.Operations.Api.Resources;
using TJ.Operations.Common.Enums;
using Microsoft.AspNetCore.Http;
using TJ.Operations.Queues;
using TJ.Operations.Queues.Models.EventMessageData;
using TJ.Operations.Common.AppSettings.Api;
using Microsoft.Extensions.Options;
using TJ.Operations.Api.Models.Extensions;
using common = TJ.Platform.Common.Models;
using BookingStatus = TJ.Operations.Common.Enums.BookingStatus;
using TJ.Operations.Api.Extensions;
using TJ.Operations.Api.Models.Dbo;
using TJ.Operations.Api.Services.Metadata;
using TJ.Operations.Integration.Services.Stripe;
using TJ.Operations.Api.Helpers;
using TJ.Platform.Common.Models;
using System.Text;
using TJ.Operations.Common.Services.Caching;
using OtpNet;
using TJ.Operations.Integration.Services.Notifications;
using TJ.Operations.Integration.AppSettings;
using TJ.Operations.Common.Helpers;
using System.Text.RegularExpressions;
using TJ.Platform.Common.Models.Responses;
using TJ.Operations.Queues.Models;
using TJ.Operations.Common.Constants;
using TJ.Platform.Common.Helpers;
using TJ.Platform.Common.Queues.Models.Messages.CoreV2;
using TJ.Platform.Common.Enums;
using TJ.Platform.Common.Constants;
using TJ.Operations.Api.Models.Enums;

namespace TJ.Operations.Api.Services
{
    public class BookingManagementService : ServiceBase<BookingManagementService>, IBookingManagementService
    {
        private readonly JrnyOperationsSettings _jrnyOperationsSettings;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IMapper _mapper;
        private readonly IAccountManager _accountManager;
        private readonly ICheckInOutManager _checkInOutManager;
        private readonly IDriverManager _driverManager;
        private readonly IOperationsBookingManager _operationsBookingManager;
        private readonly IAddressRepository _addressRepository;
        private readonly IAccountRepository _accountRepository;
        private readonly IAssetPackagePricingRepository _assetPackagePricingRepository;
        private readonly IAccountCheckHistoryRepository _accountCheckHistoryRepository;
        private readonly IBookingRepository _bookingRepository;
        private readonly IBookingParameterCaptureRepository _bookingParameterCaptureRepository;
        private readonly IBookingAssetTelematicsDataRepository _bookingAssetTelematicsDataRepository;
        private readonly IExternalAccountRepository _externalAccountRepository;
        private readonly IBookingOpsStatusRepository _bookingOpsStatusRepository;
        private readonly IConditionCaptureRepository _conditionCaptureRepository;
        private readonly IDriverRepository _driverRepository;
        private readonly IAssetManagementService _assetManagementService;
        private readonly IVoucherService _voucherService;
        private readonly IOperationsEventQueue _eventQueue;
        private readonly IEarlyEndBookingReasonRepository _earlyEndBookingReasonRepository;
        private readonly IStripeService _stripeService;
        private readonly ITagManagementService _tagManagementService;
        private readonly ITagEntityManagementService _tagEntityManagementService;
        private readonly ISMSService _smsService;
        private readonly ICustomerManagementService _customerManagementService;
        private readonly ExternalDriverSettings _externalDriverSettings;
        private readonly IBasketManager _basketManager;
        private readonly IExtraOptionRepository _extraOptionRepository;
        private readonly IVoucherManager _voucherManager;
        private readonly IBasketRepository _basketRepository;
        private readonly IBillingManager _billingManager;
        private readonly ISessionService _sessionService;
        private readonly IUserContext _userContext; //Eventually add this to service base 
        private readonly ITenantRepository _tenantRepository;
        private readonly ICoreV2MessageQueueClient _coreV2MessageQueueClient;
        private readonly IAssetRepository _assetRepository;
        private readonly IAssetAvailabilityService _assetAvailabilityService;
        private readonly IImageStorageService _imageStorageService;
        private readonly JrnyCoreSettings _coreSettings;

        public BookingManagementService(OperationsContext dbContext,
            IHttpContextAccessor httpContextAccessor,
            ILogger<BookingManagementService> logger,
            IOptions<JrnyCoreSettings> coreSettings,
            IOptions<JrnyOperationsSettings> jrnyOperationsSettings,
            IOptions<ExternalDriverSettings> externalDriverSettings,
            IHttpClientFactory httpClientFactory,
            IMapper mapper,
            IAccountManager accountManager,
            ICheckInOutManager checkInOutManager,
            IDriverManager driverManager,
            IOperationsBookingManager operationsBookingManager,
            IAddressRepository addressRepository,
            IBookingRepository bookingRepository,
            IBookingAssetTelematicsDataRepository bookingAssetTelematicsDataRepository,
            IDriverRepository driverRepository,
            IAccountRepository accountRepository,
            IAssetManagementService assetManagementService,
            IAssetPackagePricingRepository assetPackagePricingRepository,
            IAccountCheckHistoryRepository accountCheckHistoryRepository,
            IExternalAccountRepository externalAccountRepository,
            IVoucherService voucherService,
            IOperationsEventQueue eventQueue,
            IEarlyEndBookingReasonRepository earlyEndBookingReasonRepository,
            IStripeService stripeService,
            ITagManagementService tagManagementService,
            ITagEntityManagementService tagEntityManagementService,
            ICustomerManagementService customerManagementService,
            ISMSService smsService,
            IBasketManager basketManager,
            IExtraOptionRepository extraOptionRepository,
            IVoucherManager voucherManager,
            IBasketRepository basketRepository,
            IBillingManager billingManager,
            ISessionService sessionService,
            IBookingOpsStatusRepository bookingOpsStatusRepository,
            ITenantRepository tenantRepository,
            IUserContext userContext,
            ICoreV2MessageQueueClient coreV2MessageQueueClient,
            IAssetRepository assetRepository,
            IBookingParameterCaptureRepository bookingParameterCaptureRepository,
            IConditionCaptureRepository conditionCaptureRepository,
            IAssetAvailabilityService assetAvailabilityService,
            IImageStorageService imageStorageService) : base(dbContext, logger, httpContextAccessor, coreSettings)
        {
            _coreSettings = coreSettings.Value ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(JrnyCoreSettings)} was null");
            _jrnyOperationsSettings = jrnyOperationsSettings.Value ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(JrnyOperationsSettings)} was null");
            _externalDriverSettings = externalDriverSettings.Value ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(ExternalDriverSettings)} was null");
            _httpClientFactory = httpClientFactory ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IHttpClientFactory)} was null");
            _mapper = mapper ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(Mapper)} was null");
            _accountManager = accountManager ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(AccountManager)} was null");
            _checkInOutManager = checkInOutManager ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(CheckInOutManager)} was null");
            _driverManager = driverManager ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(DriverManager)} was null");
            _operationsBookingManager = operationsBookingManager ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(OperationsBookingManager)} was null");
            _addressRepository = addressRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(AddressRepository)} was null");
            _bookingRepository = bookingRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(BookingRepository)} was null");
            _bookingParameterCaptureRepository = bookingParameterCaptureRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(BookingParameterCaptureRepository)} was null");
            _bookingAssetTelematicsDataRepository = bookingAssetTelematicsDataRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(BookingAssetTelematicsDataRepository)} was null");
            _driverRepository = driverRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(DriverRepository)} was null");
            _accountRepository = accountRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(AccountRepository)} was null");
            _assetManagementService = assetManagementService ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IAssetManagementService)} was null");
            _assetPackagePricingRepository = assetPackagePricingRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(AssetPackagePricingRepository)} was null");
            _accountCheckHistoryRepository = accountCheckHistoryRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(AccountCheckHistoryRepository)} was null");
            _conditionCaptureRepository = conditionCaptureRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(ConditionCaptureRepository)} was null");
            _externalAccountRepository = externalAccountRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(ExternalAccountRepository)} was null");
            _voucherService = voucherService ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IVoucherService)} was null");
            _eventQueue = eventQueue ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(OperationsEventQueue)} was null");
            _earlyEndBookingReasonRepository = earlyEndBookingReasonRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(EarlyEndBookingReasonRepository)} was null");
            _stripeService = stripeService ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IStripeService)} was null");
            _tagManagementService = tagManagementService ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(ITagManagementService)} was null");
            _tagEntityManagementService = tagEntityManagementService ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(ITagEntityManagementService)} was null");
            _smsService = smsService ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(smsService)} was null");
            _customerManagementService = customerManagementService ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(customerManagementService)} was null");
            _basketManager = basketManager ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IBasketManager)} was null");
            _extraOptionRepository = extraOptionRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IExtraOptionRepository)} was null");
            _voucherManager = voucherManager ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IVoucherManager)} was null");
            _basketRepository = basketRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IBasketRepository)} was null");
            _billingManager = billingManager ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IBillingManager)} was null");
            _sessionService = sessionService ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(ISessionService)} was null");
            _bookingOpsStatusRepository = bookingOpsStatusRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IBookingOpsStatusRepository)} was null");
            _tenantRepository = tenantRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(ITenantRepository)} was null");
            _assetRepository = assetRepository ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IAssetRepository)} was null");
            _userContext = userContext ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IUserContext)} was null");
            _coreV2MessageQueueClient = coreV2MessageQueueClient ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(ICoreV2MessageQueueClient)} was null");
            _assetAvailabilityService = assetAvailabilityService ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IAssetAvailabilityService)} was null");
            _imageStorageService = imageStorageService ?? throw new ArgumentNullException($"[{nameof(BookingManagementService)}.Constructor] : {nameof(IImageStorageService)} was null"); ;
        }

        public async Task<ApiResponse<NewBookingResponse>> AddBookingAsync(NewBookingOrRequestDto newBooking, bool userSignedIn, Guid owningTenantId, Guid globalTenantId)
        {
            try
            {
                var tenantId = owningTenantId;
                if (_coreSettings.UseTenantIdFromBookingAsset)
                {
                    var asset = await _assetRepository.GetByIdAsync(newBooking.AssetId);
                    if (asset == null)
                    {
                        return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83940 };
                    }
                    tenantId = asset.TenantId;
                    _logger.LogDebug("[AddBookingAsync] - Creating booking for Account {AccountId} using TenantId {TenantId} from Asset {AssetId}", newBooking.AccountId, tenantId, newBooking.AssetId);
                }

                Driver driver = null;
                if (newBooking.UseDefaultDriver)
                {
                    driver = await _driverRepository.GetDefaultForAccountAsync(newBooking.AccountId, tenantId);
                }
                else
                {
                    driver = await _driverRepository.GetByIdAsync(newBooking.DriverId, tenantId);
                }

                if (driver == null)
                {
                    _logger.LogDebug("[AddBookingAsync] - Unable to find driver for Account {AccountId} using TenantId {TenantId}", newBooking.AccountId, tenantId);
                    return new ApiResponse<common.Responses.NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83884 };
                }

                var account = await _accountRepository.GetByIdAsync(newBooking.AccountId, tenantId);
                if (account == null)
                {
                    _logger.LogDebug("[AddBookingAsync] - Unable to find Account {AccountId} using TenantId {TenantId}", newBooking.AccountId, tenantId);
                    return new ApiResponse<common.Responses.NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E84061 };
                }

                if (!driver.DrivingLicenceValidated)
                {
                    _logger.LogWarning("[AddBookingAsync] - Driving Licence not Validated. Failed to create a new booking for accountId: {accountId}, driverId: {driverId}, tenantId: {tenantId}", account.Id, driver.Id, tenantId);
                    return new ApiResponse<common.Responses.NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83700 };
                }

                if (account.Suspended)
                {
                    _logger.LogWarning("[AddBookingAsync] - Account suspended. Failed to create a new booking for accountId: {accountId}, driverId: {driverId}, tenantId: {tenantId}", account.Id, driver.Id, tenantId);

                    return new ApiResponse<common.Responses.NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83701 };
                }

                var bookingExtraOptions = new List<ExtraOption>();
                if (newBooking.BookingExtras != null && newBooking.BookingExtras.Any())
                {
                    var extraOptions = await _extraOptionRepository.GetByIdsAsync(tenantId, newBooking.BookingExtras.Select(e => e.ExtraOptionId), true);
                    if (extraOptions.Count() != newBooking.BookingExtras.Count())
                    {
                        _logger.LogWarning("[AddBookingAsync] - Unable to find one or more extraoptions specified in request payload for AccountId: {accountId}, DriverId: {driverId}, TenantId: {tenantId}", account.Id, driver.Id, tenantId);
                        return new ApiResponse<common.Responses.NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E84062 };
                    }
                    bookingExtraOptions = extraOptions.ToList();
                }

                var includesRetailPricingPackage = newBooking.AssetPackagePricingId.HasValue
                                                    && newBooking.AssetPackagePricingId != Guid.Empty;

                //Check to see if the booking has an associated pricing package
                Models.Dbo.AssetPackagePricing assetPricing = null;
                if (includesRetailPricingPackage)
                {
                    assetPricing = await _assetPackagePricingRepository.GetByIdAsync(newBooking.AssetPackagePricingId.Value, tenantId);
                    if (assetPricing == null)
                    {
                        //check to see if the pricing and package information belongs to the global tenant
                        if (globalTenantId != Guid.Empty)
                        {
                            assetPricing = await _assetPackagePricingRepository.GetByIdAsync(newBooking.AssetPackagePricingId.Value, globalTenantId);
                        }
                    }

                    if (assetPricing == null)
                    {
                        _logger.LogDebug("[AddBookingAsync] - Unable to find asset pricing for Account {AccountId} using TenantId {TenantId}", newBooking.AccountId, tenantId);
                        return new ApiResponse<common.Responses.NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83703 };
                    }
                }

                List<string> voucherCodes = ReadVoucherCodesFromDto(newBooking);
                _logger.LogDebug("[AddBookingAsync] - {voucherCodeCount} voucher codes on new booking. AccountId: {accountId}, DriverId: {driverId}, TenantId: {tenantId}", voucherCodes.Count, account.Id, driver.Id, tenantId);

                var discountAndVouchers = await GetVouchersAndDiscountedPriceAsync(voucherCodes, assetPricing, tenantId, globalTenantId);
                _logger.LogDebug("[AddBookingAsync] - Price for new booking is: {price} and discounted price with voucher codes is: {discountedPrice}. AccountId: {accountId}, DriverId: {driverId}, TenantId: {tenantId}", assetPricing.Price, discountAndVouchers.Item1, account.Id, driver.Id, tenantId);

                if (voucherCodes.Any())
                {
                    //If vouchers submitted are no longer valid then reject bookingrequest
                    if (voucherCodes.Count != discountAndVouchers.Item2.Count)
                    {
                        _logger.LogWarning("[AddBookingAsync] - Valid vouchers did not match vouchers submitted on new booking. AccountId: {accountId}, DriverId: {driverId}, TenantId: {tenantId}", account.Id, driver.Id, tenantId);
                        return new ApiResponse<common.Responses.NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83709 };
                    }
                }

                HttpClient httpClient = _httpClientFactory.CreateClient("CoreApiClient");
                httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
                var url = string.Format("{0}{1}", httpClient.BaseAddress, "booking");

                // TODO: NEED TO SORT OUT START LONG/LAT WHEN UI IS IN PLACE               
                var bookingEndDate = DurationHelper.CalculateEndDateFromDuration(assetPricing.DurationInHours, newBooking.DurationUnit, newBooking.StartDateTime, _jrnyOperationsSettings.LocalRegionTimeZoneId);
                _logger.LogTrace("[AddBookingAsync] - Booking end date calculated as: {bookingEndDate} based on a start date of {bookingStartDate}, duration hours: {durationHours}, duration unit: {durationUnit}. AccountId: {accountId}, DriverId: {driverId}, TenantId: {tenantId}", bookingEndDate, newBooking.StartDateTime, assetPricing.DurationInHours, newBooking.DurationUnit, account.Id, driver.Id, tenantId);

                newBooking.DriverId = driver.Id;

                var newBookingRequest = new NewBookingOrRequest()
                {
                    StartDate = newBooking.StartDateTime,
                    EstimatedEndDate = bookingEndDate,
                    StartLongitude = 0.0,
                    StartLatitude = 0.0,
                    EstimatedReturnLongitude = 0.0,
                    EstimatedReturnLatitude = 0.0,
                    AssetId = newBooking.AssetId,
                    ExternalId = newBooking.DriverId.ToString(),
                    DrivingLicenceNumber = driver.DrivingLicence ?? driver.Id.ToString(),
                    DriverName = string.Format("{0} {1} {2}", driver.Title, driver.Firstname, driver.Surname),
                    DriverTelephoneNumber = driver.MobileTelephone,
                    OtherData = "",
                    ExternalReference = newBooking.ExternalReference,
                    Notes = newBooking.OperationalNotes,
                    UsageTypeId = newBooking.UsageTypeId
                };

                var assetMake = "";
                var assetModel = "";
                var assetVariant = "";
                var bookingReference = "";
                var assetReference = "";
                var tenantName = "";
                var assetImageUrl = "";


                //Once CoreV1 architecture is phased out this if block will not be needed.
                if (_coreSettings.CoreV2Enabled)
                {
                    _logger.LogDebug("[AddBookingAsync] - CoreV2 enabled so skipping core booking creation via HTTP...");

                    var opsAsset = await _assetRepository.GetByIdAsync(newBooking.AssetId);
                    if (opsAsset == null)
                    {
                        _logger.LogError("[AddBookingAsync] - Unable to create booking request, no asset in ops with id {assetId}", newBooking.AssetId);
                        return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E84121 };
                    }

                    var bookingTenant = await _tenantRepository.GetByIdAsync(tenantId);
                    if (opsAsset == null)
                    {
                        _logger.LogError("[AddBookingAsync] - Unable to create booking request, no booking tenant in ops with id {tenantId}", tenantId);
                        return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E84122 };
                    }

                    var assetOwnerTenant = await _tenantRepository.GetByIdAsync(opsAsset.TenantId);
                    if (opsAsset == null)
                    {
                        _logger.LogError("[AddBookingAsync] - Unable to create booking request, no asset owner tenant in ops with id {tenantId}", opsAsset.TenantId);
                        return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E84122 };
                    }

                    assetMake = opsAsset.Make;
                    assetModel = opsAsset.Model;
                    assetVariant = opsAsset.Variant;
                    assetReference = opsAsset.Identifier;
                    tenantName = bookingTenant.Label;
                    assetImageUrl = opsAsset.PropertyValues.FirstOrDefault(a => a.Master_AssetPropertyId == AssetPropertyIds.ASSET_IMAGE_URL)?.Value;
                    bookingReference = BookingReferenceHelper.GenerateBookingReference(tenantName, assetOwnerTenant.Label, "0001"); //TODO: Set location hub correctly
                }
                else
                {
                    _logger.LogDebug("[AddBookingAsync] - CoreV2 disabled so creating core booking via HTTP...");

                    string json = JsonConvert.SerializeObject(newBookingRequest, Formatting.None);
                    _logger.LogDebug("[AddBookingAsync] - Creating booking from Operational UI: {bookingData}", json);
                    var response = await httpClient.PostAsync(url, new StringContent(json, System.Text.Encoding.UTF8, "application/json"));

                    if (!response.IsSuccessStatusCode)
                    {
                        return new ApiResponse<NewBookingResponse>() { Success = false, Code = ((int)response.StatusCode).ToString() };
                    }

                    var responseContent = await response.Content.ReadAsStringAsync();
                    var apiResponse = JsonConvert.DeserializeObject<ApiResponse<common.Responses.NewBookingResponse>>(responseContent);
                    var newBookingResponse = apiResponse.Result;

                    if (apiResponse.Result == null)
                    {
                        return apiResponse;
                    }

                    assetMake = newBookingResponse.AssetMake;
                    assetModel = newBookingResponse.AssetModel;
                    assetVariant = newBookingResponse.AssetDescription;
                    bookingReference = newBookingResponse.BookingReference;
                    assetReference = newBookingResponse.AssetReference;
                    tenantName = newBookingResponse.ProviderLabel;
                    assetImageUrl = newBookingResponse.AssetImageUrl;
                }

                //handle delivery or collection addresses 
                if (string.IsNullOrEmpty(newBooking.DeliveryAddress))
                {
                    newBooking.DeliveryAddress = driver.Address.FormattedAddress();
                }
                if (!newBooking.DeliveryDate.HasValue)
                {
                    newBooking.DeliveryDate = newBooking.StartDateTime;
                }

                if (string.IsNullOrEmpty(newBooking.CollectionAddress))
                {
                    newBooking.CollectionAddress = driver.Address.FormattedAddress();
                }
                if (!newBooking.CollectionDate.HasValue)
                {
                    newBooking.CollectionDate = bookingEndDate;
                }

                var populatedBookingExtras = await _operationsBookingManager.PopulateBookingExtrasAsync(newBooking.BookingExtras, bookingExtraOptions, tenantId, assetMake, assetModel, assetVariant);

                var assetPricingSnapshot = await _operationsBookingManager.CreateAssetPricingSnapshot(assetMake, assetModel, assetVariant, tenantId, assetPricing.Mileage);

                var bookingId = await _operationsBookingManager.CreateOperationsBookingSummaryRecordAsync(newBooking.AssetId,
                    bookingReference,
                    assetMake,
                    assetModel,
                    assetVariant,
                    assetReference,
                    assetImageUrl,
                    tenantName,
                    newBooking.StartDateTime,
                    bookingEndDate,
                    Operations.Common.Enums.NetworkRecordType.Booking,
                    newBooking.DriverId,
                    newBooking.AccountId,
                    newBooking.OperationalNotes,
                    newBooking.DeliveryAddress,
                    newBooking.CollectionAddress,
                    newBooking.DeliveryDate,
                    newBooking.CollectionDate,
                    assetPricing,
                    discountAndVouchers.Item1,
                    newBooking.DurationUnit,
                    tenantId,
                    populatedBookingExtras,
                    assetPricingSnapshot,
                    createBufferRecord: true);

                _logger.LogDebug("[AddBookingAsync] - Creating basket for booking with id: {bookingId} and tenantId: {tenantId}...", bookingId, tenantId);
                var basketItems = _basketManager.CreateBasketItemsForBooking(assetPricing, discountAndVouchers, populatedBookingExtras);
                await _basketManager.AddBookingBasketAsync(bookingId, basketItems, tenantId);

                if (driver.IsLead)
                {
                    _driverManager.PromoteDriverFromLead(driver, tenantId);
                }

                if (account.IsLead)
                {
                    _accountManager.PromoteAccountFromLead(account, tenantId);
                    await RaiseCreateJrnyIdpUserEventAsync(account.Id, driver.Id, bookingReference, account.EmailAddress, GetClientId(), tenantId);
                }

                var result = await CompleteUnitOfWork();
                await IncrementAndSnapshotVouchers(tenantId, discountAndVouchers, bookingReference, result);
                await CheckForReferralVoucherOnBookingAsync(discountAndVouchers.Item2, tenantId, globalTenantId);

                var bookingRecord = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference);
                await _eventQueue.BookingApprovedEventAsync(new BookingEventData
                {
                    AccountId = bookingRecord.AccountId,
                    DriverId = bookingRecord.DriverId,
                    BookingRef = bookingRecord.UniqueReference,
                    AssetId = bookingRecord.AssetId,
                    TenantId = tenantId,
                    Url = GetBackOfficeBookingUri(bookingRecord.UniqueReference),
                    BookingId = bookingRecord.Id
                });

                if (_coreSettings.CoreV2Enabled)
                {
                    await _coreV2MessageQueueClient.CreateBookingAsync(new NewBookingOrRequestMessageData()
                    {
                        AccountId = account.Id,
                        TenantId = tenantId,
                        StartDate = newBooking.StartDateTime,
                        EstimatedEndDate = bookingEndDate,
                        StartLongitude = 0.0,
                        StartLatitude = 0.0,
                        EstimatedReturnLongitude = 0.0,
                        EstimatedReturnLatitude = 0.0,
                        AssetId = newBooking.AssetId,
                        ExternalId = newBooking.DriverId.ToString(),
                        DrivingLicenceNumber = driver.DrivingLicence ?? driver.Id.ToString(),
                        DriverName = string.Format("{0} {1} {2}", driver.Title, driver.Firstname, driver.Surname),
                        DriverTelephoneNumber = driver.MobileTelephone,
                        OtherData = "",
                        ExternalReference = newBooking.ExternalReference,
                        Notes = newBooking.OperationalNotes,
                        UsageTypeId = newBooking.UsageTypeId,
                        IsVehicleExchange = newBooking.IsVehicleExchange,
                        VehicleExchangeExistingUniqueReference = newBooking.VehicleExchangeBookingReference,
                        BookingRef = bookingReference
                    }, tenantId);
                }

                return new ApiResponse<common.Responses.NewBookingResponse>()
                {
                    Success = result,
                    Result = new NewBookingResponse()
                    {
                        AssetDescription = assetVariant,
                        AssetMake = assetMake,
                        AssetModel = assetModel,
                        AssetId = newBooking.AssetId,
                        StartDate = newBooking.StartDateTime,
                        EndDate = newBooking.EndDateTime,
                        AssetImageUrl = assetImageUrl,
                        AssetReference = assetReference,
                        BookingReference = bookingReference,
                        ProviderLabel = tenantName
                    }
                };
            }
            catch (Exception exception)
            {
                _logger.LogError("[AddBookingAsync] - Unexpected Exception creating new booking: Exception: {exception}", exception);
                return new ApiResponse<common.Responses.NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        public async Task<ApiResponse<NewBookingResponse>> AddBookingRequestAsync(NewBookingOrRequestDto newBooking, bool userSignedIn, Guid tenantId, Guid globalTenantId)
        {
            try
            {
                var driver = await _driverRepository.GetByIdAsync(newBooking.DriverId, tenantId);
                if (driver == null)
                {
                    return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83884 };
                }

                var account = await _accountRepository.GetByIdAsync(newBooking.AccountId, tenantId);
                if (account == null)
                {
                    return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E84061 };
                }

                var bookingExtraOptions = new List<ExtraOption>();
                if (newBooking.BookingExtras != null && newBooking.BookingExtras.Any())
                {
                    var extraOptions = await _extraOptionRepository.GetByIdsAsync(tenantId, newBooking.BookingExtras.Select(e => e.ExtraOptionId), true);
                    if (extraOptions.Count() != newBooking.BookingExtras.Count())
                    {
                        _logger.LogWarning("[AddBookingRequestAsync] - Unable to find one or more extraoptions specified in request payload for AccountId: {accountId}, DriverId: {driverId}, TenantId: {tenantId}", account.Id, driver.Id, tenantId);
                        return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E84062 };
                    }
                    bookingExtraOptions = extraOptions.ToList();
                }

                var includesRetailPricingPackage = newBooking.AssetPackagePricingId.HasValue && newBooking.AssetPackagePricingId != Guid.Empty;

                //Check to see if the booking has an associated pricing package
                Models.Dbo.AssetPackagePricing assetPricing = null;
                if (includesRetailPricingPackage)
                {
                    assetPricing = await _assetPackagePricingRepository.GetByIdAsync(newBooking.AssetPackagePricingId.Value, tenantId);
                    if (assetPricing == null)
                    {
                        //check to see if the pricing and package information belongs to the global tenant
                        if (globalTenantId != Guid.Empty)
                        {
                            assetPricing = await _assetPackagePricingRepository.GetByIdAsync(newBooking.AssetPackagePricingId.Value, globalTenantId);
                        }
                    }

                    if (assetPricing == null)
                    {
                        return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83703 };
                    }
                }

                List<string> voucherCodes = ReadVoucherCodesFromDto(newBooking);
                _logger.LogDebug("[AddBookingRequestAsync] - {voucherCodeCount} voucher codes on new booking request. AccountId: {accountId}, DriverId: {driverId}, TenantId: {tenantId}", voucherCodes.Count, account.Id, driver.Id, tenantId);

                var discountAndVouchers = await GetVouchersAndDiscountedPriceAsync(voucherCodes, assetPricing, tenantId, globalTenantId);
                _logger.LogDebug("[AddBookingRequestAsync] - Price for new booking request is: {price} and discounted price with voucher codes is: {discountedPrice}. AccountId: {accountId}, DriverId: {driverId}, TenantId: {tenantId}", assetPricing.Price, discountAndVouchers.Item1, account.Id, driver.Id, tenantId);

                if (voucherCodes.Any())
                {
                    //If vouchers submitted are no longer valid then reject bookingrequest
                    if (voucherCodes.Count != discountAndVouchers.Item2.Count)
                    {
                        _logger.LogWarning("[AddBookingRequestAsync] - Valid vouchers did not match vouchers submitted on new booking request. AccountId: {accountId}, DriverId: {driverId}, TenantId: {tenantId}", account.Id, driver.Id, tenantId);
                        return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83709 };
                    }
                }

                HttpClient httpClient = _httpClientFactory.CreateClient("CoreApiClient");
                httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
                var url = string.Format("{0}{1}", httpClient.BaseAddress, "bookingrequest");

                // TODO: NEED TO SORT OUT TELEPHONE NUMBER, OTHER DATA AND START LONG/LAT WHEN UI IS IN PLACE

                var bookingEndDate = DurationHelper.CalculateEndDateFromDuration(assetPricing.DurationInHours, newBooking.DurationUnit, newBooking.StartDateTime, _jrnyOperationsSettings.LocalRegionTimeZoneId);
                _logger.LogTrace("[AddBookingRequestAsync] - Booking end date calculated as: {bookingEndDate} based on a start date of {bookingStartDate}, duration hours: {durationHours}, duration unit: {durationUnit}. AccountId: {accountId}, DriverId: {driverId}, TenantId: {tenantId}", bookingEndDate, newBooking.StartDateTime, assetPricing.DurationInHours, newBooking.DurationUnit, account.Id, driver.Id, tenantId);

                var newBookingRequest = new NewBookingOrRequest()
                {
                    StartDate = newBooking.StartDateTime,
                    EstimatedEndDate = bookingEndDate,
                    StartLongitude = 0.0,
                    StartLatitude = 0.0,
                    EstimatedReturnLongitude = 0.0,
                    EstimatedReturnLatitude = 0.0,
                    AssetId = newBooking.AssetId,
                    ExternalId = newBooking.DriverId.ToString(),
                    DrivingLicenceNumber = driver.DrivingLicence ?? driver.Id.ToString(),
                    DriverName = string.Format("{0} {1} {2}", driver.Title, driver.Firstname, driver.Surname),
                    DriverTelephoneNumber = driver.MobileTelephone,
                    OtherData = "",
                    ExternalReference = newBooking.ExternalReference,
                    Notes = newBooking.OperationalNotes,
                    UsageTypeId = newBooking.UsageTypeId,
                    IsVehicleExchange = newBooking.IsVehicleExchange,
                    VehicleExchangeExistingUniqueReference = newBooking.VehicleExchangeBookingReference
                };

                var assetMake = "";
                var assetModel = "";
                var assetVariant = "";
                var bookingReference = "";
                var assetReference = "";
                var tenantName = "";
                var assetImageUrl = "";

                //Once CoreV1 architecture is phased out this if block will not be needed.
                if (_coreSettings.CoreV2Enabled)
                {
                    _logger.LogDebug("[AddBookingRequestAsync] - CoreV2 enabled so skipping core booking creation via HTTP...");

                    var opsAsset = await _assetRepository.GetByIdAsync(newBooking.AssetId);
                    if (opsAsset == null)
                    {
                        _logger.LogError("[AddBookingRequestAsync] - Unable to create booking request, no asset in ops with id {assetId}", newBooking.AssetId);
                        return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E84121 };
                    }

                    var bookingTenant = await _tenantRepository.GetByIdAsync(tenantId);
                    if (opsAsset == null)
                    {
                        _logger.LogError("[AddBookingRequestAsync] - Unable to create booking request, no booking tenant in ops with id {tenantId}", tenantId);
                        return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E84122 };
                    }

                    var assetOwnerTenant = await _tenantRepository.GetByIdAsync(opsAsset.TenantId);
                    if (opsAsset == null)
                    {
                        _logger.LogError("[AddBookingRequestAsync] - Unable to create booking request, no asset owner tenant in ops with id {tenantId}", opsAsset.TenantId);
                        return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E84122 };
                    }

                    assetMake = opsAsset.Make;
                    assetModel = opsAsset.Model;
                    assetVariant = opsAsset.Variant;
                    assetReference = opsAsset.Identifier;
                    tenantName = bookingTenant.Label;
                    assetImageUrl = opsAsset.PropertyValues.FirstOrDefault(a => a.Master_AssetPropertyId == AssetPropertyIds.ASSET_IMAGE_URL)?.Value;
                    bookingReference = newBooking.IsVehicleExchange ?
                        BookingReferenceHelper.GetIncrementedBookingReference(newBooking.VehicleExchangeBookingReference) :
                        BookingReferenceHelper.GenerateBookingReference(tenantName, assetOwnerTenant.Label, "0001"); //TODO: Set location hub correctly
                }
                else
                {
                    _logger.LogDebug("[AddBookingRequestAsync] - CoreV2 disabled creating booking in core via HTTP...");
                    string json = JsonConvert.SerializeObject(newBookingRequest, Formatting.None);
                    var response = await httpClient.PostAsync(url, new StringContent(json, System.Text.Encoding.UTF8, "application/json"));

                    if (!response.IsSuccessStatusCode)
                    {
                        return new ApiResponse<NewBookingResponse>() { Success = false, Code = ((int)response.StatusCode).ToString() };
                    }

                    var responseContent = await response.Content.ReadAsStringAsync();
                    var apiResponse = JsonConvert.DeserializeObject<ApiResponse<common.Responses.NewBookingResponse>>(responseContent);

                    var newBookingResponse = apiResponse.Result;

                    if (apiResponse.Result == null)
                    {
                        return apiResponse;
                    }

                    assetMake = newBookingResponse.AssetMake;
                    assetModel = newBookingResponse.AssetModel;
                    assetVariant = newBookingResponse.AssetDescription;
                    bookingReference = newBookingResponse.BookingReference;
                    assetReference = newBookingResponse.AssetReference;
                    tenantName = newBookingResponse.ProviderLabel;
                    assetImageUrl = newBookingResponse.AssetImageUrl;
                }

                //handle delivery or collection addresses 
                if (string.IsNullOrEmpty(newBooking.DeliveryAddress))
                {
                    newBooking.DeliveryAddress = driver.Address.FormattedAddress();
                }
                if (!newBooking.DeliveryDate.HasValue)
                {
                    newBooking.DeliveryDate = newBooking.StartDateTime;
                }

                if (string.IsNullOrEmpty(newBooking.CollectionAddress))
                {
                    newBooking.CollectionAddress = driver.Address.FormattedAddress();
                }
                if (!newBooking.CollectionDate.HasValue)
                {
                    newBooking.CollectionDate = bookingEndDate;
                }

                var populatedBookingExtras = await _operationsBookingManager.PopulateBookingExtrasAsync(newBooking.BookingExtras, bookingExtraOptions, tenantId, assetMake, assetModel, assetVariant);

                var assetPricingSnapshot = await _operationsBookingManager.CreateAssetPricingSnapshot(assetMake, assetModel, assetVariant, tenantId, assetPricing.Mileage);

                var bookingId = await _operationsBookingManager.CreateOperationsBookingSummaryRecordAsync(newBooking.AssetId,
                    bookingReference,
                    assetMake,
                    assetModel,
                    assetVariant,
                    assetReference,
                    assetImageUrl,
                    tenantName,
                    newBooking.StartDateTime,
                    newBooking.EndDateTime,
                    Common.Enums.NetworkRecordType.Request,
                    newBooking.DriverId,
                    newBooking.AccountId,
                    newBooking.OperationalNotes,
                    newBooking.DeliveryAddress,
                    newBooking.CollectionAddress,
                    newBooking.DeliveryDate,
                    newBooking.CollectionDate,
                    assetPricing,
                    discountAndVouchers.Item1,
                    newBooking.DurationUnit,
                    tenantId,
                    populatedBookingExtras,
                    assetPricingSnapshot);

                _logger.LogDebug("[AddBookingRequestAsync] - Creating basket for booking with id: {bookingId} and tenantId: {tenantId}...", bookingId, tenantId);
                var basketItems = _basketManager.CreateBasketItemsForBooking(assetPricing, discountAndVouchers, populatedBookingExtras);
                await _basketManager.AddBookingBasketAsync(bookingId, basketItems, tenantId);

                if (driver.IsLead)
                {
                    _driverManager.PromoteDriverFromLead(driver, tenantId);
                }

                if (account.IsLead)
                {
                    _accountManager.PromoteAccountFromLead(account, tenantId);
                    await RaiseCreateJrnyIdpUserEventAsync(account.Id, driver.Id, bookingReference, account.EmailAddress, GetClientId(), tenantId);
                }

                // Record CreditKudos information if available.
                if (newBooking.CreditKudos != null)
                {
                    await _accountCheckHistoryRepository.AddAsync(new Models.Dbo.AccountCheckHistory()
                    {
                        Id = Guid.NewGuid(),
                        CreatedDate = DateTime.UtcNow,
                        AccountCheckTypeId = (int)Common.Enums.AccountCheckType.CreditKudos,
                        AccountId = newBooking.AccountId,
                        TenantId = tenantId,
                        Result = newBooking.CreditKudos.isVerified.ToString(),
                        FullResult = JsonConvert.SerializeObject(newBooking.CreditKudos)
                    });
                }

                var result = await CompleteUnitOfWork();

                await IncrementAndSnapshotVouchers(tenantId, discountAndVouchers, bookingReference, result);

                if (_coreSettings.CoreV2Enabled)
                {
                    await _coreV2MessageQueueClient.CreateBookingRequestAsync(new NewBookingOrRequestMessageData()
                    {
                        AccountId = account.Id,
                        TenantId = tenantId,
                        StartDate = newBooking.StartDateTime,
                        EstimatedEndDate = bookingEndDate,
                        StartLongitude = 0.0,
                        StartLatitude = 0.0,
                        EstimatedReturnLongitude = 0.0,
                        EstimatedReturnLatitude = 0.0,
                        AssetId = newBooking.AssetId,
                        ExternalId = newBooking.DriverId.ToString(),
                        DrivingLicenceNumber = driver.DrivingLicence ?? driver.Id.ToString(),
                        DriverName = string.Format("{0} {1} {2}", driver.Title, driver.Firstname, driver.Surname),
                        DriverTelephoneNumber = driver.MobileTelephone,
                        OtherData = "",
                        ExternalReference = newBooking.ExternalReference,
                        Notes = newBooking.OperationalNotes,
                        UsageTypeId = newBooking.UsageTypeId,
                        IsVehicleExchange = newBooking.IsVehicleExchange,
                        VehicleExchangeExistingUniqueReference = newBooking.VehicleExchangeBookingReference,
                        BookingRef = bookingReference
                    }, tenantId);
                }

                if (!userSignedIn)
                {
                    //raise new customer event 
                    await _eventQueue.CustomerOnboardedEventAsync(new BaseTenantMessageData
                    {
                        AccountId = account.Id,
                        BookingRef = bookingReference,
                        DriverId = driver.Id,
                        Url = GetBackOfficeCustomerUri(account.Id),
                        TenantId = tenantId
                    });
                }
                else
                {
                    _logger.LogDebug("[AddBookingRequestAsync] - Skipping raising customerOnboardedEvent and DVLACheckEvent as booking is for a signedInUser. AccountId: {accountId}", account.Id);
                }

                return new ApiResponse<NewBookingResponse>()
                {
                    Success = result,
                    Result = new NewBookingResponse()
                    {
                        AssetDescription = assetVariant,
                        AssetMake = assetMake,
                        AssetModel = assetModel,
                        AssetId = newBooking.AssetId,
                        StartDate = newBooking.StartDateTime,
                        EndDate = newBooking.EndDateTime,
                        AssetImageUrl = assetImageUrl,
                        AssetReference = assetReference,
                        BookingReference = bookingReference,
                        ProviderLabel = tenantName
                    }
                };
            }
            catch (Exception exception)
            {
                _logger.LogError("[AddBookingAsync] - Unexpected Exception creating booking request: Exception: {exception}", exception);
                return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        public async Task<ApiResponse<NewBookingResponse>> AddBookingExchangeRequestAsync(NewBookingExchangeRequestDto newBooking, Guid tenantId, Guid globalTenantId)
        {
            try
            {
                var booking = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(newBooking.UniqueReference, tenantId);
                if (booking == null)
                {
                    _logger.LogWarning("[AddBookingExchangeRequestAsync] - Could not find booking with bookingRef: {ref} for tenantId: {tenantId}", booking.UniqueReference, tenantId);
                    return new ApiResponse<NewBookingResponse>()
                    {
                        Success = false,
                        Code = OperationsApiStatusCodes.E83810
                    };
                }

                // Is there a booking already placed for this end date?
                var bookingRefPrefix = booking.UniqueReference.Split('_')[0];
                var hasExchange = _bookingRepository.DoesFutureBookingOrRequestExist(bookingRefPrefix, booking.EndDate, tenantId);
                if (hasExchange)
                {
                    _logger.LogWarning("[AddBookingExchangeRequestAsync] - Placed booking already exists with a unique reference starting with: {ref} for tenantId: {tenantId}", booking.UniqueReference, tenantId);
                    return new ApiResponse<NewBookingResponse>()
                    {
                        Success = false,
                        Code = OperationsApiStatusCodes.E84073
                    };
                }

                var assetPricing = await _assetPackagePricingRepository.GetByIdAsync(newBooking.AssetPackagePricingId, tenantId);
                if (assetPricing == null)
                {
                    if (globalTenantId != Guid.Empty)
                    {
                        assetPricing = await _assetPackagePricingRepository.GetByIdAsync(newBooking.AssetPackagePricingId, globalTenantId);
                    }
                }

                if (assetPricing == null)
                {
                    _logger.LogWarning("[AddBookingExchangeRequestAsync] - Asset package pricing with id: {assetPackagePricingId} for tenantId: {tenantId} or globalTenantId: {globalTenantId} does not exist",
                        newBooking.AssetPackagePricingId,
                        tenantId,
                        globalTenantId
                    );
                    return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83861 };
                }

                // Load existing bookings using the booking reference prefix to work out the new reference.
                var existingBookings = await _bookingRepository.GetAllBookingsOrRequestsByBookingReferencePrefixAsync(bookingRefPrefix, tenantId);
                var latestBooking = existingBookings.OrderByDescending(b => b.CreatedDate).First();
                var startDateTime = booking.EndDate;
                var durationUnit = newBooking.DurationUnit;
                var endDateTime = DurationHelper.CalculateEndDateFromDuration(assetPricing.DurationInHours, durationUnit, startDateTime, _jrnyOperationsSettings.LocalRegionTimeZoneId);
                var model = new NewBookingOrRequestDto()
                {
                    VoucherCodes = newBooking.VoucherCodes,
                    AccountId = booking.AccountId,
                    DriverId = booking.DriverId,
                    StartDateTime = startDateTime,
                    EndDateTime = endDateTime,
                    UsageTypeId = Guid.Empty, // Core will select the default if empty guid supplied.
                    AssetId = newBooking.AssetId,
                    DurationUnit = booking.DurationUnit,
                    AssetPackagePricingId = newBooking.AssetPackagePricingId,
                    BookingExtras = newBooking.BookingExtras,
                    IsVehicleExchange = true,
                    VehicleExchangeBookingReference = latestBooking.UniqueReference
                };

                _logger.LogInformation("[AddBookingExchangeRequestAsync] - Adding booking request with vehicle exchange for bookingRef: {ref} and tenantId: {tenantId}", booking.UniqueReference, tenantId);

                var resp = await AddBookingRequestAsync(model, true, tenantId, globalTenantId);

                if (resp.Success)
                {
                    var newbookingRequest = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(resp.Result.BookingReference, tenantId);
                    await _tagEntityManagementService.CheckAndAddEntityTagAsync(Tags.BOOKING_EXCHANGED, tenantId, booking.Id, Models.Enums.EntityType.Booking);
                    await _tagEntityManagementService.CheckAndAddEntityTagAsync(Tags.BOOKING_EXCHANGE, tenantId, booking.Id, Models.Enums.EntityType.Booking);
                }

                return resp;
            }
            catch (Exception exception)
            {
                _logger.LogError("[AddBookingExchangeRequestAsync] - Unexpected Exception creating booking request: Exception: {exception}", exception);
                return new ApiResponse<NewBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        public async Task<ApiResponse<NewBookingResponse>> AddBookingRequestForUserAsync(NewBookingOrRequestDto newBookingRequest, Guid accountId, Guid userId, Guid tenantId, Guid globalTenantId)
        {
            try
            {
                if (accountId != userId)
                {
                    _logger.LogWarning("[AddBookingRequestForUserAsync] - User: {userId} has access to account: {accountId}", userId, accountId);
                    return new ApiResponse<common.Responses.NewBookingResponse>() { Result = null, Success = false, Code = OperationsApiStatusCodes.E83943 };
                }

                if (newBookingRequest.AccountId != userId)
                {
                    _logger.LogWarning("[AddBookingRequestForUserAsync] - User: {userId} has access to account on booking. Account: {accountId}", userId, newBookingRequest.AccountId);
                    return new ApiResponse<common.Responses.NewBookingResponse>() { Result = null, Success = false, Code = OperationsApiStatusCodes.E83943 };
                }

                var account = await _accountRepository.GetByIdAsync(accountId, tenantId);
                var accountDriver = account.AccountDrivers.Single(); // Intentially breaks if multiple driver records appear in the future.

                newBookingRequest.AccountId = account.Id;
                newBookingRequest.DriverId = accountDriver.DriverId;

                _logger.LogDebug("[AddBookingRequestForUserAsync] - Calling AddBookingRequest for user: {userId}", userId);
                return await AddBookingRequestAsync(newBookingRequest, true, tenantId, globalTenantId);
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "[AddBookingRequestForUserAsync] - Unexpected Exception creating new booking request.");
                return new ApiResponse<common.Responses.NewBookingResponse>() { Result = null, Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        public async Task<ApiResponse<NewBookingResponse>> AddBookingForUserAsync(NewBookingOrRequestDto newBookingRequest, Guid accountId, Guid userId, Guid tenantId, Guid globalTenantId)
        {
            try
            {
                if (accountId != userId)
                {
                    _logger.LogWarning("[AddBookingForUserAsync] - User: {userId} has no access to account: {accountId}", userId, accountId);
                    return new ApiResponse<common.Responses.NewBookingResponse>() { Result = null, Success = false, Code = OperationsApiStatusCodes.E83943 };
                }

                if (newBookingRequest.AccountId != userId)
                {
                    _logger.LogWarning("[AddBookingForUserAsync] - User: {userId} has no access to account on booking. Account: {accountId}", userId, newBookingRequest.AccountId);
                    return new ApiResponse<common.Responses.NewBookingResponse>() { Result = null, Success = false, Code = OperationsApiStatusCodes.E83943 };
                }

                var account = await _accountRepository.GetByIdAsync(accountId, tenantId);
                var accountDriver = account.AccountDrivers.Single(); // Intentially breaks if multiple driver records appear in the future.

                newBookingRequest.AccountId = account.Id;
                newBookingRequest.DriverId = accountDriver.DriverId;

                _logger.LogDebug("[AddBookingForUserAsync] - Calling AddBooking for user: {userId}", userId);
                return await AddBookingAsync(newBookingRequest, true, tenantId, globalTenantId);
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "[AddBookingForUserAsync] - Unexpected Exception creating new booking request.");
                return new ApiResponse<common.Responses.NewBookingResponse>() { Result = null, Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        public async Task<ApiResponse<bool>> CheckInAsync(CheckInOutParameterCaptureDto checkInParameters, List<IFormFile> images, string bookingReference, Guid assetId, Guid bookingId, Guid tenantId)
        {
            try
            {
                if (_coreSettings.CoreV2Enabled)
                {
                    Guid newBookingParameterCaptureId = Guid.NewGuid();
                    var newBookingParameterCaptureDbo = _mapper.Map<BookingParameterCapture>(checkInParameters, opts =>
                    {
                        opts.Items["bookingParameterCaptureId"] = newBookingParameterCaptureId;
                        opts.Items["bookingId"] = bookingId;
                        opts.Items["tenantId"] = tenantId;
                        opts.Items["bookingParameterCaptureTypeId"] = (int)Common.Enums.BookingParameterCaptureType.CheckIn;
                    });

                    await _bookingParameterCaptureRepository.AddAsync(newBookingParameterCaptureDbo);

                    List<string> imageUrls = await _checkInOutManager.ProcessCheckinImagesAsync(images, bookingReference, Common.Enums.BookingParameterCaptureType.CheckIn, tenantId);
                    Dictionary<Guid, string> conditionCaptures = new Dictionary<Guid, string>();

                    foreach (var imageUrl in imageUrls)
                    {
                        Guid conditionCaptureId = Guid.NewGuid();
                        var conditionCaptureDbo = new ConditionCapture()
                        {
                            Id = conditionCaptureId,
                            BookingParameterCaptureId = newBookingParameterCaptureId,
                            CreatedDate = DateTime.UtcNow,
                            EntryDate = DateTime.UtcNow,
                            ImageUrl = imageUrl,
                        };

                        conditionCaptures.Add(conditionCaptureId, imageUrl);
                        await _conditionCaptureRepository.AddAsync(conditionCaptureDbo);
                    }

                    await _checkInOutManager.CreateBookingHandoverEventAsync(imageUrls, bookingReference, bookingId, assetId, Common.Enums.BookingParameterCaptureType.CheckIn, tenantId);
                    await _operationsBookingManager.UpdateOperationsBookingStatusAndActualEndAsync(bookingReference, BookingStatus.Started, tenantId, null, null);

                    await CompleteUnitOfWorkAsync();

                    await _coreV2MessageQueueClient.BeginBookingJourneyAsync(new BeginOrEndBookingJourneyMessageData()
                    {
                        BookingParameterCaptureId = newBookingParameterCaptureId,
                        BookingRef = bookingReference,
                        BookingId = bookingId,
                        Condition = checkInParameters.Condition,
                        Cleanliness = checkInParameters.Cleanliness,
                        FuelLevel = checkInParameters.FuelLevel,
                        Latitude = checkInParameters.Latitude,
                        Longitude = checkInParameters.Longitude,
                        Notes = checkInParameters.Notes,
                        Mileage = checkInParameters.Mileage,
                        ConditionCaptures = conditionCaptures,
                        TenantId = tenantId
                    }, tenantId);
                }
                else
                {
                    // This block can go when CoreV2 is rolled out.

                    HttpClient httpClient = _httpClientFactory.CreateClient("CoreApiClient");
                    httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
                    var url = string.Format("{0}{1}{2}", httpClient.BaseAddress, "booking/beginjourney/", bookingReference);

                    var checkinRequest = new CheckInOutParameterCaptureRequest()
                    {
                        Cleanliness = checkInParameters.Cleanliness,
                        Condition = checkInParameters.Condition,
                        FuelLevel = checkInParameters.FuelLevel,
                        Longitude = checkInParameters.Longitude,
                        Latitude = checkInParameters.Latitude,
                        Mileage = checkInParameters.Mileage,
                        Notes = checkInParameters.Notes,
                        Type = Common.Enums.BookingParameterCaptureType.CheckIn
                    };

                    string json = JsonConvert.SerializeObject(checkinRequest, Formatting.None);
                    var response = await httpClient.PostAsync(url, new StringContent(json, System.Text.Encoding.UTF8, "application/json"));

                    _logger.LogDebug("[CheckInAsync] - response from core has status code: {statusCode}. bookingRef: {bookingReference}", response.StatusCode, bookingReference);

                    if (!response.IsSuccessStatusCode)
                    {
                        return new ApiResponse<bool>() { Success = false, Code = ((int)response.StatusCode).ToString() };
                    }

                    var responseContent = await response.Content.ReadAsStringAsync();
                    ApiResponse<bool> apiResponse = JsonConvert.DeserializeObject<ApiResponse<bool>>(responseContent);

                    if (apiResponse.Success == false)
                    {
                        return apiResponse;
                    }
                    
                    List<string> imageUrls = await _checkInOutManager.ProcessCheckinImagesAsync(images, bookingReference, Common.Enums.BookingParameterCaptureType.CheckIn, tenantId);
                    await _checkInOutManager.CreateBookingHandoverEventAsync(imageUrls, bookingReference, bookingId, assetId, Common.Enums.BookingParameterCaptureType.CheckIn, tenantId);
                    await _operationsBookingManager.UpdateOperationsBookingStatusAndActualEndAsync(bookingReference, BookingStatus.Started, tenantId, null, null);
                    await CompleteUnitOfWorkAsync();
                }

                return new ApiResponse<bool>() { Success = true, Result = true };
            }
            catch (Exception exception)
            {
                _logger.LogError("[CheckinAsync] - Unexpected Exception checking into booking: {bookingReference}. Exception: {exception}", bookingReference, exception);
                return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        public async Task<ApiResponse<bool>> CheckInAsync(CheckInOutParameterCaptureDto checkInParameters, List<IFormFile> images, string bookingReference, Guid assetId, Guid bookingId, Guid tenantId, Guid sessionId)
        {
            _logger.LogDebug("[CheckInAsync] - starting booking for an authenticated booking session: sessionId: {sessionId} and bookingReference: {bookingReference}", sessionId, bookingReference);

            var failureResp = new ApiResponse<bool>();

            // Check session id.           
            var validSessionResp = _sessionService.IsSessionValid(sessionId, Models.Enums.SessionType.TestDrive, bookingReference);
            if (!validSessionResp.Result)
            {
                failureResp.Code = OperationsApiStatusCodes.E83867;
                _logger.LogDebug("[CheckInAsync] - cannot start booking for an authenticated booking session: sessionId: {sessionId} and bookingReference: {bookingReference}. Session validation failed.", sessionId, bookingReference);
                return failureResp;
            }

            return await CheckInAsync(checkInParameters, images, bookingReference, assetId, bookingId, tenantId);
        }

        public async Task<ApiResponse<bool>> CheckOutAsync(CheckInOutParameterCaptureDto checkInParameters, List<IFormFile> images, string bookingReference, Guid assetId, Guid bookingId, Guid tenantId)
        {
            try
            {
                if (_coreSettings.CoreV2Enabled)
                {
                    Guid newBookingParameterCaptureId = Guid.NewGuid();
                    var newBookingParameterCaptureDbo = _mapper.Map<BookingParameterCapture>(checkInParameters, opts =>
                    {
                        opts.Items["bookingParameterCaptureId"] = newBookingParameterCaptureId;
                        opts.Items["bookingId"] = bookingId;
                        opts.Items["tenantId"] = tenantId;
                        opts.Items["bookingParameterCaptureTypeId"] = (int)Common.Enums.BookingParameterCaptureType.CheckOut;
                    });

                    await _bookingParameterCaptureRepository.AddAsync(newBookingParameterCaptureDbo);

                    List<string> imageUrls = await _checkInOutManager.ProcessCheckinImagesAsync(images, bookingReference, Common.Enums.BookingParameterCaptureType.CheckOut, tenantId);
                    Dictionary<Guid, string> conditionCaptures = new Dictionary<Guid, string>();
                    DateTime actualEndDate = DateTime.UtcNow;
                    int? currentBufferLengthInHours = await _operationsBookingManager.GetCurrentBufferLengthInHoursAsync(bookingReference, tenantId);

                    foreach (var imageUrl in imageUrls)
                    {
                        Guid conditionCaptureId = Guid.NewGuid();
                        var conditionCaptureDbo = new ConditionCapture()
                        {
                            Id = conditionCaptureId,
                            BookingParameterCaptureId = newBookingParameterCaptureId,
                            CreatedDate = DateTime.UtcNow,
                            EntryDate = DateTime.UtcNow,
                            ImageUrl = imageUrl,
                        };

                        conditionCaptures.Add(conditionCaptureId, imageUrl);
                        await _conditionCaptureRepository.AddAsync(conditionCaptureDbo);
                    }

                    await _checkInOutManager.CreateBookingHandoverEventAsync(imageUrls, bookingReference, bookingId, assetId, Common.Enums.BookingParameterCaptureType.CheckOut, tenantId);
                    await _operationsBookingManager.UpdateOperationsBookingStatusAndActualEndAsync(bookingReference, BookingStatus.Completed, tenantId, actualEndDate, currentBufferLengthInHours);

                    await CompleteUnitOfWorkAsync();

                    await _coreV2MessageQueueClient.EndBookingJourneyAsync(new BeginOrEndBookingJourneyMessageData()
                    {
                        BookingParameterCaptureId = newBookingParameterCaptureId,
                        BookingRef = bookingReference,
                        BookingId = bookingId,
                        Condition = checkInParameters.Condition,
                        Cleanliness = checkInParameters.Cleanliness,
                        FuelLevel = checkInParameters.FuelLevel,
                        Latitude = checkInParameters.Latitude,
                        Longitude = checkInParameters.Longitude,
                        Notes = checkInParameters.Notes,
                        Mileage = checkInParameters.Mileage,
                        ConditionCaptures = conditionCaptures,
                        ActualEndDate = actualEndDate,
                        BufferOverrideValue = currentBufferLengthInHours,
                        TenantId = tenantId
                    }, tenantId);
                }
                else
                {
                    // This block can go when CoreV2 is rolled out.

                    HttpClient httpClient = _httpClientFactory.CreateClient("CoreApiClient");
                    httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
                    var url = string.Format("{0}{1}{2}", httpClient.BaseAddress, "booking/endjourney/", bookingReference);
                    DateTime actualEndDate = DateTime.UtcNow;
                    int? currentBufferLengthInHours = await _operationsBookingManager.GetCurrentBufferLengthInHoursAsync(bookingReference, tenantId);

                    var checkinRequest = new CheckInOutParameterCaptureRequest()
                    {
                        Cleanliness = checkInParameters.Cleanliness,
                        Condition = checkInParameters.Condition,
                        FuelLevel = checkInParameters.FuelLevel,
                        Longitude = checkInParameters.Longitude,
                        Latitude = checkInParameters.Latitude,
                        Mileage = checkInParameters.Mileage,
                        Notes = checkInParameters.Notes,
                        ActualEndDate = actualEndDate,
                        BufferOverrideValue = currentBufferLengthInHours,
                        Type = Common.Enums.BookingParameterCaptureType.CheckOut
                    };

                    string json = JsonConvert.SerializeObject(checkinRequest, Formatting.None);
                    var response = await httpClient.PostAsync(url, new StringContent(json, System.Text.Encoding.UTF8, "application/json"));

                    _logger.LogDebug("[CheckOutAsync] - response from core has status code: {statusCode}. bookingRef: {bookingReference}", response.StatusCode, bookingReference);

                    if (!response.IsSuccessStatusCode)
                    {
                        return new ApiResponse<bool>() { Success = false, Code = ((int)response.StatusCode).ToString() };
                    }

                    var responseContent = await response.Content.ReadAsStringAsync();
                    ApiResponse<bool> apiResponse = JsonConvert.DeserializeObject<ApiResponse<bool>>(responseContent);

                    if (apiResponse.Success == false)
                    {
                        return apiResponse;
                    }

                    List<string> imageUrls = await _checkInOutManager.ProcessCheckinImagesAsync(images, bookingReference, Common.Enums.BookingParameterCaptureType.CheckOut, tenantId);
                    await _checkInOutManager.CreateBookingHandoverEventAsync(imageUrls, bookingReference, bookingId, assetId, Common.Enums.BookingParameterCaptureType.CheckOut, tenantId);
                    await _operationsBookingManager.UpdateOperationsBookingStatusAndActualEndAsync(bookingReference, BookingStatus.Completed, tenantId, actualEndDate, currentBufferLengthInHours);
                    await CompleteUnitOfWorkAsync();
                }

                return new ApiResponse<bool>() { Success = true, Result = true };
            }
            catch (Exception exception)
            {
                _logger.LogError("[CheckOutAsync] - Unexpected Exception checking out of booking: {bookingReference}. Exception: {exception}", bookingReference, exception);
                return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        public async Task<ApiResponse<bool>> CheckOutAsync(CheckInOutParameterCaptureDto checkInParameters, List<IFormFile> images, string bookingReference, Guid assetId, Guid bookingId, Guid tenantId, Guid sessionId)
        {
            _logger.LogDebug("[CheckOutAsync] - ending booking for authenticated booking session: sessionId: {sessionId} and bookingReference: {bookingReference}", sessionId, bookingReference);

            var failureResp = new ApiResponse<bool>();

            // Check session id.
            var validSessionResp = _sessionService.IsSessionValid(sessionId, Models.Enums.SessionType.TestDrive, bookingReference);
            if (!validSessionResp.Result)
            {
                failureResp.Code = OperationsApiStatusCodes.E83867;
                _logger.LogDebug("[CheckOutAsync] - cannot end booking for authenticated booking session: sessionId: {sessionId} and bookingReference: {bookingReference}. Session validation failed.", sessionId, bookingReference);
                return failureResp;
            }

            return await CheckOutAsync(checkInParameters, images, bookingReference, assetId, bookingId, tenantId);
        }

        public bool DoesBookingExist(string bookingReference, Guid tenantId)
        {
            try
            {
                if (_bookingRepository.DoesBookingOrRequestRecordExist(bookingReference, tenantId))
                {
                    _logger.LogDebug("[DoesRecordExist] - booking with reference: {bookingReference} and tenantId: {tenantId} found.", bookingReference, tenantId);
                    return true;
                }
                else
                {
                    _logger.LogDebug("[DoesRecordExist] - No booking with reference: {bookingReference} and tenantId: {tenantId} found.", bookingReference, tenantId);
                    return false;
                }
            }
            catch (Exception exception)
            {
                _logger.LogError("[DoesBookingExist] - Unexpected exception, returning false. Exception: {exception}", exception);
                return false;
            }
        }

        public async Task<ApiResponse<BookingParameterCaptureDto>> GetBookingCheckInRecordAsync(string bookingReference, Guid tenantId)
        {
            try
            {
                _logger.LogDebug("[GetBookingCheckinRecordAsync] - Retriving booking checkIn data for booking: {bookingReference}. TenantId: {tenanId}", bookingReference, tenantId);

                var booking = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference, tenantId);
                if (booking == null)
                {
                    _logger.LogWarning("[GetBookingCheckinRecordAsync] - Could not find booking with bookingRef: {ref} for tenantId: {tenantId}", bookingReference, tenantId);
                    return new ApiResponse<BookingParameterCaptureDto>() { Success = false, Result = null, Code = OperationsApiStatusCodes.E83860 };
                }

                if (_coreSettings.CoreV2Enabled)
                {
                    var bookingParameterCaptureDbo = await _bookingParameterCaptureRepository.GetByBookingIdAsync(booking.Id, Common.Enums.BookingParameterCaptureType.CheckIn, tenantId);

                    if (bookingParameterCaptureDbo == null)
                    {
                        _logger.LogWarning("[GetBookingCheckinRecordAsync] - Could not find booking parameterCaptureRecord with bookingRef: {ref} for tenantId: {tenantId}", booking.UniqueReference, tenantId);
                        return new ApiResponse<BookingParameterCaptureDto>() { Success = false, Code = OperationsApiStatusCodes.E85024 };
                    }

                    var result = _mapper.Map<BookingParameterCaptureDto>(bookingParameterCaptureDbo);
                    return new ApiResponse<BookingParameterCaptureDto>() { Success = true, Result = result };
                }
                else
                {
                    HttpClient httpClient = _httpClientFactory.CreateClient("CoreApiClient");
                    httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
                    var url = string.Format("{0}{1}{2}", httpClient.BaseAddress, "checkin/", bookingReference);

                    var response = await httpClient.GetAsync(url);

                    if (response.IsSuccessStatusCode)
                    {
                        var responseContent = await response.Content.ReadAsStringAsync();
                        common.Responses.NetworkRecordParameterCaptureResponse parameterCapture = JsonConvert.DeserializeObject<common.Responses.NetworkRecordParameterCaptureResponse>(responseContent);
                        var bookingParameters = _mapper.Map<BookingParameterCaptureDto>(parameterCapture);
                        return new ApiResponse<BookingParameterCaptureDto>() { Success = true, Result = bookingParameters };
                    }
                    else
                    {
                        int statusCode = (int)response.StatusCode;
                        return new ApiResponse<BookingParameterCaptureDto>() { Success = false, Code = statusCode.ToString() };
                    }
                }                
            }
            catch (Exception exception)
            {
                _logger.LogError("[GetBookingCheckinRecordAsync] - Unexpected Exception retriving booking check in data: Exception: {exception}", exception);
                return new ApiResponse<BookingParameterCaptureDto>() { Result = null, Success = false, Code = OperationsApiStatusCodes.E83000};
            }
        }

        public async Task<ApiResponse<BookingParameterCaptureDto>> GetBookingCheckOutRecordAsync(string bookingReference, Guid tenantId)
        {
            try
            {
                _logger.LogDebug("[GetBookingCheckOutRecordAsync] - Retriving booking check out data for booking: {bookingReference}. TenantId: {tenanId}", bookingReference, tenantId);

                var booking = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference, tenantId);
                if (booking == null)
                {
                    _logger.LogWarning("[GetBookingCheckOutRecordAsync] - Could not find booking with bookingRef: {ref} for tenantId: {tenantId}", bookingReference, tenantId);
                    return new ApiResponse<BookingParameterCaptureDto>() { Success = false, Result = null, Code = OperationsApiStatusCodes.E83860 };
                }

                if (_coreSettings.CoreV2Enabled)
                {
                    var bookingParameterCaptureDbo = await _bookingParameterCaptureRepository.GetByBookingIdAsync(booking.Id, Common.Enums.BookingParameterCaptureType.CheckOut, tenantId);

                    if (bookingParameterCaptureDbo == null)
                    {
                        _logger.LogWarning("[GetBookingCheckOutRecordAsync] - Could not find booking parameterCaptureRecord with bookingRef: {ref} for tenantId: {tenantId}", booking.UniqueReference, tenantId);
                        return new ApiResponse<BookingParameterCaptureDto>() { Success = false, Code = OperationsApiStatusCodes.E85024 };
                    }

                    foreach (var capture in bookingParameterCaptureDbo?.ConditionCaptures)
                    {
                        if (!string.IsNullOrEmpty(capture.ImageUrl))
                        {
                            var image = await _imageStorageService.GetImageAsync(capture.ImageUrl);
                            if (image == null)
                            {
                                _logger.LogWarning("[GetBookingCheckOutRecordAsync] - Could not get secure image url for {ImageUrl}", capture.ImageUrl);
                            }
                            else
                            {
                                capture.ImageUrl = image;
                            }
                        }
                    }

                    var result = _mapper.Map<BookingParameterCaptureDto>(bookingParameterCaptureDbo);
                    return new ApiResponse<BookingParameterCaptureDto>() { Success = true, Result = result };
                }
                else
                {

                    HttpClient httpClient = _httpClientFactory.CreateClient("CoreApiClient");
                    httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
                    var url = string.Format("{0}{1}{2}", httpClient.BaseAddress, "checkout/", bookingReference);

                    var response = await httpClient.GetAsync(url);

                    if (response.IsSuccessStatusCode)
                    {
                        var responseContent = await response.Content.ReadAsStringAsync();
                        common.Responses.NetworkRecordParameterCaptureResponse parameterCapture = JsonConvert.DeserializeObject<common.Responses.NetworkRecordParameterCaptureResponse>(responseContent);
                        var bookingParameters = _mapper.Map<BookingParameterCaptureDto>(parameterCapture);
                        return new ApiResponse<BookingParameterCaptureDto>() { Success = true, Result = bookingParameters };
                    }
                    else
                    {
                        int statusCode = (int)response.StatusCode;
                        return new ApiResponse<BookingParameterCaptureDto>() { Success = false, Code = statusCode.ToString() };
                    }
                }
            }
            catch (Exception exception)
            {
                _logger.LogError("[GetBookingCheckOutRecordAsync] - Unexpected Exception retriving booking check out data: Exception: {exception}", exception);
                return new ApiResponse<BookingParameterCaptureDto>() { Result = null, Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        public async Task<ApiResponse<BookingDetailsDto>> GetBookingDetailsAsync(string bookingReference, Guid tenantId)
        {
            try
            {
                _logger.LogDebug("[GetBookingDetailsAsync] - Get booking details for booking: {bookingReference}. TenantId: {tenantId}", bookingReference, tenantId);

                var bookingRecord = new BookingDetailsDto();

                //Get the summary object to fill in the missing driver and account informtiton
                var bookingSummary = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference, tenantId);

                if (bookingSummary == null)
                {
                    _logger.LogWarning("[GetBookingDetailsAsync] - Couldn't find booking summary item for: {bookingReference}. TenantId: {tenanId}", bookingReference, tenantId);
                    return new ApiResponse<BookingDetailsDto>() { Success = false, Result = null, Code = "404" }; //TODO: Proper code needs using once controllers have been refactored to use response handler
                }

                //Once corev1 obselete, this if statement can be removed
                if (_coreSettings.CoreV2Enabled)
                {
                    _logger.LogDebug("[GetBookingDetailsAsync] - CoreV2 enabled so only get booking details from ops for booking: {bookingReference}. TenantId: {tenantId}", bookingReference, tenantId);
                    var asset = await _assetRepository.GetByIdAsync(bookingSummary.AssetId);
                    if (asset == null)
                    {
                        _logger.LogError("[GetBookingDetailsAsync] - No asset found in ops with id: {assetId}, unable to get booking details with reference: {bookingReference}, TenantId: {tenantId}", bookingSummary.AssetId, bookingReference, tenantId);
                        return new ApiResponse<BookingDetailsDto>() { Success = false, Code = OperationsApiStatusCodes.E84121 };
                    }

                    var tenant = await _tenantRepository.GetByIdAsync(bookingSummary.TenantId);
                    if (tenant == null)
                    {
                        _logger.LogWarning("[GetBookingDetailsAsync] - No tenant found in ops with id: {tenantId}, unable to load booking source for booking ref: {bookingReference}", tenantId);
                    }

                    bookingRecord.Asset = new AssetSummaryDto()
                    {
                        Make = asset.Make,
                        Model = asset.Model,
                        Variant = asset.Variant,
                        Registration = asset.Identifier,
                        ImageUrl = asset.PropertyValues.FirstOrDefault(a => a.Master_AssetPropertyId == AssetPropertyIds.ASSET_IMAGE_URL)?.Value
                    };

                    bookingRecord.StartDate = bookingSummary.StartDate;
                    bookingRecord.EndDate = bookingSummary.EndDate;
                    bookingRecord.Status = (BookingStatus)bookingSummary.BookingStatusId;
                    bookingRecord.UniqueReference = bookingSummary.UniqueReference;
                    bookingRecord.ExternalReference = bookingSummary.ExternalReference;
                    bookingRecord.ProviderLabel = tenant?.Label;
                }
                else
                {
                    HttpClient httpClient = _httpClientFactory.CreateClient("CoreApiClient");
                    httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
                    string url = string.Format("{0}{1}{2}", httpClient.BaseAddress, "booking/", bookingReference);

                    var response = await httpClient.GetAsync(url);

                    if (!response.IsSuccessStatusCode)
                    {
                        return new ApiResponse<BookingDetailsDto>() { Success = false, Code = ((int)response.StatusCode).ToString() };
                    }

                    var responseContent = await response.Content.ReadAsStringAsync();
                    NetworkRecordSummaryResponse networkRecordSummary = JsonConvert.DeserializeObject<NetworkRecordSummaryResponse>(responseContent);
                    bookingRecord = _mapper.Map<BookingDetailsDto>(networkRecordSummary);
                }

                // Format Driver information
                string driverName = string.Format("{0} {1}", bookingSummary.Driver.Firstname, bookingSummary.Driver.Surname);
                Address driverAddress = _mapper.Map<Address>(await _addressRepository.GetByIdAsync(bookingSummary.Driver.AddressId, tenantId));

                bookingRecord.Id = bookingSummary.Id;
                bookingRecord.AccountId = bookingSummary.AccountId;
                bookingRecord.AccountName = bookingSummary.Account.AccountName;
                bookingRecord.DriverName = driverName;
                bookingRecord.DriverAddress = driverAddress.FormattedAddress();
                bookingRecord.Mileage = bookingSummary.Mileage;
                bookingRecord.Notes = bookingSummary.OperationalNotes;
                bookingRecord.Price = bookingSummary.Price ?? 0;
                bookingRecord.DiscountedPrice = bookingSummary.DiscountedPrice ?? 0;
                bookingRecord.InitialPayment = bookingSummary.InitialPayment ?? 0;
                bookingRecord.CollectionAddress = bookingSummary.CollectionAddress;
                bookingRecord.CollectionDate = bookingSummary.CollectionDate;
                bookingRecord.DeliveryAddress = bookingSummary.DeliveryAddress;
                bookingRecord.DeliveryDate = bookingSummary.DeliveryDate;
                bookingRecord.BookingVouchers = (from v in bookingSummary.Vouchers select new BookingVoucherDto() { Code = v.Code }).ToList();
                bookingRecord.ExternalBookings = (from e in bookingSummary.ExternalBookings
                                                  select new ExternalBookingDetailsDto()
                                                  {
                                                      ExternalId = e.ExternalId,
                                                      BookingId = e.BookingId,
                                                      ExternalType = (ExternalBookingIntegrationType)Enum.ToObject(typeof(ExternalBookingIntegrationType), e.ExternalTypeId)
                                                  }).ToList();

                bookingRecord.ActualEndDate = bookingSummary.ActualEndDate;
                bookingRecord.Asset.ImageUrl = bookingSummary.AssetImageUrl;

                if (!string.IsNullOrEmpty(bookingSummary.DurationUnit))
                {
                    var durationUnit = (DurationUnits)Enum.Parse(typeof(DurationUnits), bookingSummary.DurationUnit);
                    bookingRecord.DurationDescription = DurationHelper.GetPackageDuration(bookingSummary.DurationInHours, durationUnit);
                }

                if (bookingSummary.BookingOpsStatusId != null)
                {
                    bookingRecord.OpsStatus = new BookingOpsStatusDto()
                    {
                        Id = bookingSummary.BookingOpsStatus.Id,
                        Label = bookingSummary.BookingOpsStatus.Label
                    };
                }

                if (bookingSummary.ExtraFields != null)
                {
                    bookingRecord.ExtraFields = new BookingExtraFieldDto()
                    {
                        RecurringWeeklyPaymentDay = bookingSummary.ExtraFields.RecurringWeeklyPaymentDay,
                        WeeklyTotalInclCAFContribution = bookingSummary.ExtraFields.WeeklyTotalInclCAFContribution,
                        WeeklyCAFContribution = bookingSummary.ExtraFields.WeeklyCAFContribution,
                        BookingId = bookingSummary.ExtraFields.BookingId,
                        TotalCAFApplied = bookingSummary.ExtraFields.TotalCAFApplied,
                        TotalCAFFundAvailable = bookingSummary.ExtraFields.TotalCAFFundAvailable
                    };
                }

                //There should never be the situation where a booking has been made without a selected asset package
                //but just in case there is
                if (bookingSummary.AssetPackagePricingId.HasValue && bookingSummary.AssetPackagePricingId.Value != Guid.Empty)
                {
                    bookingRecord.AssetPackagePricingId = bookingSummary.AssetPackagePricingId.Value;
                }

                return new ApiResponse<BookingDetailsDto>() { Success = true, Result = bookingRecord };

            }
            catch (Exception exception)
            {
                _logger.LogError("[GetBookingDetailsAsync] - Unexpected Exception retriving booking details: Exception: {exception}", exception);
                return new ApiResponse<BookingDetailsDto>() { Code = OperationsApiStatusCodes.E83810 };
            }
        }

        public async Task<ApiResponse<BookingDetailsDto>> GetBookingDetailsAsync(string bookingReference, Guid tenantId, Guid sessionId)
        {
            _logger.LogDebug("[GetBookingDetailsAsync] - getting booking details for authenticated booking session: sessionId: {sessionId} and bookingReference: {bookingReference}", sessionId, bookingReference);

            var failureResp = new ApiResponse<BookingDetailsDto>();

            // Check session id.          
            var validSessionResp = _sessionService.IsSessionValid(sessionId, Models.Enums.SessionType.TestDrive, bookingReference);
            if (!validSessionResp.Result)
            {
                failureResp.Code = OperationsApiStatusCodes.E83867;
                _logger.LogDebug("[GetBookingDetailsAsync] - cannot get booking details for authenticated booking session: sessionId: {sessionId} and bookingReference: {bookingReference}. Session validation failed.", sessionId, bookingReference);
                return failureResp;
            }

            //session id has been validated for this booking reference
            return await GetBookingDetailsAsync(bookingReference, tenantId);
        }

        public async Task<ApiResponse<BookingOperationsDto>> GetBookingOperationsRecordAsync(string bookingReference, Guid tenantId)
        {
            BookingOperationsDto bookingOperationsDto = new BookingOperationsDto();

            try
            {
                var booking = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference, tenantId);

                if (booking != null)
                {
                    _logger.LogDebug("[GetBookingOperationsRecordAsync] - Found operational details for booking: {bookingReference}", bookingReference);
                    bookingOperationsDto.DeliveryAddress = booking.DeliveryAddress;
                    bookingOperationsDto.DeliveryDate = booking.DeliveryDate;
                    bookingOperationsDto.CollectionAddress = booking.CollectionAddress;
                    bookingOperationsDto.CollectionDate = booking.CollectionDate;
                    bookingOperationsDto.Notes = booking.OperationalNotes;
                }

                return new ApiResponse<BookingOperationsDto>()
                {
                    Success = true,
                    Result = bookingOperationsDto
                };
            }
            catch (Exception exception)
            {
                _logger.LogError("[GetBookingSummaryRecordsAsync] - Unexpected exception: {exception}", exception);
                throw;
            }
        }

        public async Task<BookingSummaryDto> GetBookingSummaryRecordAsync(string bookingRefernece, Guid tenantId)
        {
            BookingSummaryDto bookingSummaryRecord = new BookingSummaryDto();

            try
            {
                bookingSummaryRecord = _mapper.Map<BookingSummaryDto>(await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingRefernece, tenantId));
                return bookingSummaryRecord;
            }
            catch (Exception exception)
            {
                _logger.LogError("[GetBookingSummaryRecordsAsync] - Unexpected exception: {exception}", exception);
                return bookingSummaryRecord;
            }
        }

        public async Task<ApiResponse<IList<BookingSummaryDto>>> GetBookingSummaryRecordsAsync(Guid tenantId)
        {
            List<BookingSummaryDto> bookingSummaryRecords = new List<BookingSummaryDto>();

            try
            {
                bookingSummaryRecords = _mapper.Map<List<BookingSummaryDto>>(await _bookingRepository.GetAllBookingsOrRequestsAsync(tenantId));
                return new ApiResponse<IList<BookingSummaryDto>>()
                {
                    Success = true,
                    Result = bookingSummaryRecords
                };
            }
            catch (Exception exception)
            {
                _logger.LogError("[GetBookingSummaryRecordsAsync] - Unexpected exception: {exception}", exception);
                throw;
            }
        }

        public async Task<IList<BookingSummaryDto>> GetBookingSummaryRecordsForAccountAsync(Guid accountId, Guid tenantId)
        {
            List<BookingSummaryDto> bookingSummaryRecords = new List<BookingSummaryDto>();

            try
            {
                bookingSummaryRecords = _mapper.Map<List<BookingSummaryDto>>(await _bookingRepository.GetAllBookingsOrRequestsByAccountAsync(accountId, tenantId));
                return bookingSummaryRecords;
            }
            catch (Exception exception)
            {
                _logger.LogError("[GetBookingSummaryRecordsForAccountAsync] - Unexpected exception: {exception}", exception);
                return bookingSummaryRecords;
            }
        }

        public async Task<ApiResponse<IList<BookingUsageDto>>> GetBookingUsageRecordsAsync(string bookingReference, Guid tenantId)
        {
            var resp = new ApiResponse<IList<BookingUsageDto>>();

            try
            {
                var usageData = await _bookingAssetTelematicsDataRepository.GetAllByBookingReferenceAsync(bookingReference, tenantId);
                return new ApiResponse<IList<BookingUsageDto>>()
                {
                    Success = true,
                    Result = _mapper.Map<List<BookingUsageDto>>(usageData)
                };
            }
            catch (Exception exception)
            {
                _logger.LogError($"[{nameof(GetBookingUsageRecordsAsync)}] - Unexpected exception: {exception}");
                return new ApiResponse<IList<BookingUsageDto>>() { Code = OperationsApiStatusCodes.E83000 };
            }
        }

        public async Task<ApiResponse<UpdatedBookingResponse>> ApproveBookingRequestAsync(string bookingReference, UpdatedBookingOrRequestDto updatedBookingOrRequest, bool firstTimeApproval, Guid tenantId, Guid globalTenantId)
        {
            _logger.LogDebug("[ApproveBookingRequestAsync] - Beginning booking approval process for booking: {bookingReference}. Asset: {assetId}, StartDate: {startDate}, EndDate: {endDate}",
                bookingReference, updatedBookingOrRequest.AssetId, updatedBookingOrRequest.StartDateTime, updatedBookingOrRequest.EndDateTime);

            try
            {
                var existingBooking = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference, tenantId);
                var account = await _accountRepository.GetByIdAsync(existingBooking.AccountId, tenantId);

                //Get driving licence number to update in core
                var driver = account.AccountDrivers.FirstOrDefault();
                updatedBookingOrRequest.DrivingLicenceNumber = "";
                if (driver != null && !string.IsNullOrEmpty(driver.Driver.DrivingLicence))
                {
                    updatedBookingOrRequest.DrivingLicenceNumber = driver.Driver.DrivingLicence;
                }

                //Re-calc end date in case value sent from front end is not correct
                updatedBookingOrRequest.EndDateTime = DurationHelper.CalculateEndDateFromDuration(existingBooking.DurationInHours, existingBooking.DurationUnit, updatedBookingOrRequest.StartDateTime, _jrnyOperationsSettings.LocalRegionTimeZoneId);

                var assetMake = existingBooking.AssetMake;
                var assetModel = existingBooking.AssetModel;
                var assetDescription = existingBooking.AssetDescription;
                var assetReference = existingBooking.AssetReference;
                var bookingStatusId = existingBooking.BookingStatusId;
                var assetImageUrl = existingBooking.AssetImageUrl;
                var bookingTypeId = existingBooking.BookingTypeId;

                //Once CoreV1 architecture is phased out this if block will not be needed.
                if (_coreSettings.CoreV2Enabled)
                {
                    //Check asset availability
                    var tenant = await _tenantRepository.GetByIdAsync(existingBooking.TenantId);
                    var bufferValue = tenant.BufferValue;
                    var defaultOpsStatus = await _operationsBookingManager.GetDefaultOpsStatusForBookingStatus((Common.Enums.BookingStatus)bookingStatusId);
                    if (defaultOpsStatus != null && defaultOpsStatus.BufferOverrideEnabled)
                    { 
                        bufferValue = defaultOpsStatus.BufferOverride;
                    }

                    var availableRes = await _assetAvailabilityService.IsAssetAvailabileAsync(updatedBookingOrRequest.AssetId, updatedBookingOrRequest.StartDateTime, updatedBookingOrRequest.EndDateTime, bufferValue, existingBooking.UniqueReference);
                    if (!availableRes.Success)
                    {
                        _logger.LogError("[ApproveBookingRequestAsync] - failed to check asset availability for asset id: {assetId} and booking ref: {bookingReference}", updatedBookingOrRequest.AssetId, bookingReference);
                        return new ApiResponse<UpdatedBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E84130 };
                    }

                    if (!availableRes.Result)
                    {
                        _logger.LogError("[ApproveBookingRequestAsync] - asset with id: {assetId} not available. booking ref: {bookingReference}", updatedBookingOrRequest.AssetId, bookingReference);
                        return new ApiResponse<UpdatedBookingResponse>() { Success = false, Code = "409" }; //Use same code core returns if not available
                    }

                    _logger.LogDebug("[ApproveBookingRequestAsync] - CoreV2 enabled so skipping core booking update via HTTP...");

                    //Load physical asset
                    var asset = await _assetRepository.GetByIdAsync(updatedBookingOrRequest.AssetId);
                    if (asset == null)
                    {
                        _logger.LogError("[ApproveBookingRequestAsync] - No asset found in ops with id: {assetId}, unable to approve booking request with ref: {bookingReference}", updatedBookingOrRequest.AssetId, bookingReference);
                        return new ApiResponse<UpdatedBookingResponse>() { Success = false, Code = OperationsApiStatusCodes.E84121 };
                    }

                    assetMake = asset.Make;
                    assetModel = asset.Model;
                    assetDescription = asset.Variant;
                    assetReference = asset.Identifier;
                    assetImageUrl = asset.PropertyValues.FirstOrDefault(a => a.Master_AssetPropertyId == AssetPropertyIds.ASSET_IMAGE_URL)?.Value;
                }
                else
                {
                    var response = await UpdateCoreBookingRequestAsync(bookingReference, updatedBookingOrRequest, tenantId);
                    if (!response.IsSuccessStatusCode)
                    {
                        return new ApiResponse<UpdatedBookingResponse>() { Success = false, Code = ((int)response.StatusCode).ToString() };
                    }

                    var responseContent = await response.Content.ReadAsStringAsync();
                    var apiResponse = JsonConvert.DeserializeObject<ApiResponse<UpdatedBookingResponse>>(responseContent);

                    _logger.LogDebug("[ApproveBookingRequestAsync] - Response from core API when updating booking: {bookingReference}. Response: {response}", bookingReference, apiResponse);

                    if (apiResponse.Result == null)
                    {
                        return apiResponse;
                    }

                    var bookingResponse = apiResponse.Result;
                    assetMake = bookingResponse.AssetMake;
                    assetModel = bookingResponse.AssetModel;
                    assetDescription = bookingResponse.AssetDescription;
                    assetReference = bookingResponse.AssetReference;
                    assetImageUrl = bookingResponse.AssetImageUrl;
                    bookingStatusId = (int)bookingResponse.BookingStatus;
                }

                await _operationsBookingManager.UpdateOperationsBookingSummaryRecordAsync(bookingReference, bookingStatusId, tenantId, updatedBookingOrRequest.AssetId,
                    assetMake, assetModel, assetReference, assetDescription, assetImageUrl,
                    updatedBookingOrRequest.StartDateTime, updatedBookingOrRequest.EndDateTime, (int)Platform.Common.Enums.NetworkRecordType.Booking, 0,
                    createBufferRecord: true, earlyEndBookingReasonId: null);

                account.Approved = true;

                var result = await CompleteUnitOfWork();
                _logger.LogDebug("[ApproveBookingRequestAsync] - Result from updating operation's booking summary record and account id: {id} to be marked as approved: {result}", account.Id, result);

                if (!result)
                {
                    return new ApiResponse<UpdatedBookingResponse>() { Code = OperationsApiStatusCodes.E83930 };
                }

                //raise booking approved event         
                int.TryParse(existingBooking?.Mileage, out int mileageLimit);

                if (existingBooking.Vouchers.Any())
                {
                    var vouchers = await _voucherService.GetVouchersAssociatedWithBookingAsync(existingBooking.Id, tenantId);
                    await CheckForReferralVoucherOnBookingAsync(vouchers.Result, tenantId, globalTenantId);
                }

                if (firstTimeApproval)
                {
                    await RaiseCustomerApprovedEventAsync(existingBooking, account.Id, tenantId);
                    await RaiseBookingApprovedEventAsync(existingBooking, updatedBookingOrRequest.AssetId, tenantId);
                }
                else
                {
                    await RaiseBookingEditedEventAsync(existingBooking, updatedBookingOrRequest.AssetId, tenantId);
                }

                if (_coreSettings.CoreV2Enabled)
                {
                    await _coreV2MessageQueueClient.UpdateBookingAsync(new UpdateBookingMessageData()
                    {
                        BufferValue = null,
                        StartDate = updatedBookingOrRequest.StartDateTime,
                        AssetId = updatedBookingOrRequest.AssetId,
                        EndDate = updatedBookingOrRequest.EndDateTime,
                        BookingId = existingBooking.Id,
                        BookingRef = existingBooking.UniqueReference,
                        TenantId = tenantId,
                        AccountId = account.Id,
                        DriverId = existingBooking.DriverId
                    }, tenantId);
                }

                return new ApiResponse<UpdatedBookingResponse>
                {
                    Success = true,
                    Result = new UpdatedBookingResponse()
                    {
                        AssetId = updatedBookingOrRequest.AssetId,
                        StartDate = updatedBookingOrRequest.StartDateTime,
                        EndDate = updatedBookingOrRequest.EndDateTime,
                        AssetDescription = assetDescription,
                        AssetMake = assetMake,
                        AssetModel = assetModel,
                        AssetImageUrl = assetImageUrl,
                        AssetReference = assetReference,
                        BookingStatus = (NetworkRecordStatus)bookingStatusId,
                        BookingType = (Platform.Common.Enums.NetworkRecordType)bookingTypeId
                    }
                };
            }
            catch (Exception exception)
            {
                _logger.LogError("[ApproveBookingRequestAsync] - Unexpected Exception approving booking: Exception: {exception}", exception);
                return new ApiResponse<UpdatedBookingResponse> { Success = false, Code = "GENERIC FAILURE CODE" };
            }
        }

        public async Task<ApiResponse<bool>> CancelOrRejectBookingAsync(string bookingReference, bool suspendAccount, Guid tenantId, bool rejectInsteadOfCancel)
        {

            if (rejectInsteadOfCancel)
            {
                _logger.LogDebug("[CancelOrRejectBookingAsync] - Rejecting booking: {bookingReference}. Suspending account = {suspendAccount}", bookingReference, suspendAccount);
            }
            else
            {
                _logger.LogDebug("[CancelOrRejectBookingAsync] - Canceling booking: {bookingReference}. Suspending account = {suspendAccount}", bookingReference, suspendAccount);
            }

            async Task<HttpResponseMessage> CancelCoreBookingAsyncLocal()
            {
                HttpClient httpClient = _httpClientFactory.CreateClient("CoreApiClient");
                httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
                var url = string.Format("{0}{1}{2}", httpClient.BaseAddress, "booking/cancel/", bookingReference);
                var response = await httpClient.DeleteAsync(url);
                return response;
            }

            try
            {
                var booking = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference, tenantId);

                if (_coreSettings.CoreV2Enabled)
                {
                    _logger.LogInformation("[CancelOrRejectBookingAsync] - CoreV2 enabled. Skipping HTTP call to Core for bookingRef: {bookingRef} and tenantId: {tenantId}", bookingReference, tenantId);
                }
                else
                {
                    _logger.LogInformation("[CancelOrRejectBookingAsync] - CoreV2 not enabled. Making HTTP request to Core for bookingRef: {bookingRef} and tenantId: {tenantId}", bookingReference, tenantId);

                    var response = await CancelCoreBookingAsyncLocal();
                    if (response.IsSuccessStatusCode)
                    {
                        var responseContent = await response.Content.ReadAsStringAsync();
                        var apiResponse = JsonConvert.DeserializeObject<ApiResponse<bool>>(responseContent);

                        _logger.LogInformation("[CancelOrRejectBookingAsync] - Response from core API when canceling booking: {bookingReference}. Response: {response}", bookingReference, apiResponse);

                        if (apiResponse.Result == false)
                        {
                            return apiResponse;
                        }
                    }
                    else
                    {
                        if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                        {
                            return new ApiResponse<bool>() { Success = false, Code = "409" };
                        }
                        else if (response.StatusCode == System.Net.HttpStatusCode.BadRequest)
                        {
                            return new ApiResponse<bool>() { Success = false, Code = "400" };
                        }
                        else if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
                        {
                            return new ApiResponse<bool>() { Success = false, Code = "404" };
                        }
                        else
                        {
                            return new ApiResponse<bool>() { Success = false, Code = response.StatusCode.ToString() };
                        }
                    }
                }

                var bufferRec = await _bookingRepository.GetByBookingReferenceAndTypeAsync(bookingReference, Common.Enums.NetworkRecordType.Buffer, tenantId);
                if (bufferRec != null)
                {
                    bufferRec.BookingStatusId = (int)BookingStatus.Cancelled;
                }

                booking.BookingStatusId = (int)BookingStatus.Cancelled;
                var result = await CompleteUnitOfWork();

                _logger.LogInformation("[CancelOrRejectBookingAsync] - Result from operations booking summary record: {bookingRef} updated to status cancelled. Result: {result}", booking.UniqueReference, result);

                if (rejectInsteadOfCancel)
                {
                    await _eventQueue.BookingRejectedAsync(new BookingEventData
                    {
                        AccountId = booking.AccountId,
                        BookingRef = booking.UniqueReference,
                        DriverId = booking.DriverId,
                        Url = GetBackOfficeCustomerUri(booking.AccountId),
                        TenantId = tenantId
                    });
                }
                else
                {
                    await _eventQueue.BookingCancelledAsync(new BookingEventData
                    {
                        AccountId = booking.AccountId,
                        BookingRef = booking.UniqueReference,
                        DriverId = booking.DriverId,
                        Url = GetBackOfficeCustomerUri(booking.AccountId),
                        TenantId = tenantId
                    });
                }

                if (suspendAccount)
                {
                    //mark the customer as being suspended
                    var account = await _accountRepository.GetByIdAsync(booking.AccountId, tenantId);
                    account.Suspended = true;
                    result = await CompleteUnitOfWork();

                    _logger.LogInformation("[CancelOrRejectBookingAsync] - Result from account id: {id} being marked as suspended. Result: {result}", account.Id, result);

                    await _eventQueue.CustomerAccountSuspendedEventAsync(new AccountEventData
                    {
                        AccountId = booking.AccountId,
                        BookingRef = booking.UniqueReference,
                        DriverId = booking.DriverId,
                        Url = GetBackOfficeCustomerUri(booking.AccountId),
                        TenantId = tenantId
                    });
                }

                // Add message to queue to process in Core.
                if (_coreSettings.CoreV2Enabled)
                {
                    _logger.LogInformation("[CancelOrRejectBookingAsync] - Enqueuing message for processing in Core for bookingRef: {bookingRef} and tenantId: {tenantId}", bookingReference, tenantId);

                    await _coreV2MessageQueueClient.CancelOrRejectBookingRequestAsync(new BookingCancelledOrRejectedMessageData()
                    {
                        BookingId = booking.Id,
                        BookingRef = booking.UniqueReference,
                        TenantId = tenantId
                    }, tenantId);
                }

                return new ApiResponse<bool>
                {
                    Success = true,
                    Result = result,
                    Code = result == true ? OperationsApiStatusCodes.S83819 : OperationsApiStatusCodes.E83838
                };

            }
            catch (Exception exception)
            {
                _logger.LogError("[CancelOrRejectBookingAsync] - Unexpected Exception canceling booking: Exception: {exception}", exception);
                return new ApiResponse<bool> { Success = false, Code = OperationsApiStatusCodes.E83838 };
            }
        }

        public async Task<ApiResponse<bool>> UpdateBookingSummaryAsync(string bookingReference, BookingDetailsDto updatedBookingOrRequest, Guid tenantId)
        {
            try
            {
                var existingBooking = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference, tenantId);

                existingBooking.OperationalNotes = updatedBookingOrRequest.Notes;
                existingBooking.DeliveryAddress = updatedBookingOrRequest.DeliveryAddress;
                existingBooking.DeliveryDate = updatedBookingOrRequest.DeliveryDate;
                existingBooking.CollectionAddress = updatedBookingOrRequest.CollectionAddress;
                existingBooking.CollectionDate = updatedBookingOrRequest.CollectionDate;

                _bookingRepository.Update(existingBooking);

                var result = await CompleteUnitOfWork();

                return new ApiResponse<bool>()
                {
                    Success = result,
                    Result = result,
                    Code = result == true ? OperationsApiStatusCodes.S83805 : OperationsApiStatusCodes.E83805
                };
            }
            catch (Exception ex)
            {
                _logger.LogError("[UpdateAccountAsync] - Unexpected exception: {exception}", ex);

                return new ApiResponse<bool>()
                {
                    Success = false,
                    Result = false,
                    Code = OperationsApiStatusCodes.E83805
                };
            }
        }

        public async Task<IList<EarlyEndBookingReasonDto>> GetEarlyEndBookingReasonsAsync(Guid tenantId)
        {
            _logger.LogDebug("[GetEarlyEndBookingReasons] - Retrieving all EarlyEndBookingReasons...");
            var earlyEndBookingReasons = _mapper.Map<IList<EarlyEndBookingReasonDto>>(await _earlyEndBookingReasonRepository.GetAllAsync(false));
            _logger.LogDebug("[GetEarlyEndBookingReasons] - {EarlyEndBookingReasonsCount} EarlyEndBookingReasons retrieved!", earlyEndBookingReasons.Count);
            return earlyEndBookingReasons;
        }

        public async Task<ApiResponse<List<BookingOpsStatusDto>>> GetBookingOpsStatusesForBookingStatus(BookingStatus bookingStatus)
        {
            try
            {
                var opsStatuses = await _bookingOpsStatusRepository.GetByBookingStatusAsync(bookingStatus);
                _logger.LogDebug("[GetBookingOpsStatusesForBookingStatus] - Found: {opsStatusCount} ops statuses for booking status: {bookingStatus}", opsStatuses.Count(), bookingStatus);

                var opsStatusDtos = _mapper.Map<List<BookingOpsStatusDto>>(opsStatuses);

                return new ApiResponse<List<BookingOpsStatusDto>>()
                {
                    Success = true,
                    Result = opsStatusDtos
                };
            }
            catch (Exception exception)
            {
                _logger.LogError("[GetBookingOpsStatusesForBookingStatus] - Unexpected exception: {exception}", exception);
                return new ApiResponse<List<BookingOpsStatusDto>>() { Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        public async Task<ApiResponse<List<BookingOpsStatusDto>>> GetBookingOpsStatuses()
        {
            try
            {
                var opsStatuses = await _bookingOpsStatusRepository.GetAllAsync();
                _logger.LogDebug("[GetBookingOpsStatuses] - Found: {opsStatusCount} ops statuses", opsStatuses.Count());

                var opsStatusDtos = _mapper.Map<List<BookingOpsStatusDto>>(opsStatuses);

                return new ApiResponse<List<BookingOpsStatusDto>>()
                {
                    Success = true,
                    Result = opsStatusDtos
                };
            }
            catch (Exception exception)
            {
                _logger.LogError("[GetBookingOpsStatuses] - Unexpected exception: {exception}", exception);
                return new ApiResponse<List<BookingOpsStatusDto>>() { Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        /// <summary>
        /// Returns all bookings, with no restrictions
        /// </summary>
        /// <param name="tenantId"></param>
        public async Task<ApiResponse<IEnumerable<Booking>>> GetAllBookingsAsync(Guid tenantId)
        {
            try
            {
                var allBookings = await _bookingRepository.GetAllBookingsAsync(tenantId);
                return new ApiResponse<IEnumerable<Booking>>() { Success = true, Result = allBookings };
            }
            catch (Exception exception)
            {
                _logger.LogError("[GetAllBookingsAsync] - Unexpected exception: {exception}", exception);
                return new ApiResponse<IEnumerable<Booking>>() { Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        /// <summary>
        /// Gets all core network records, with no restrictions
        /// </summary>
        /// <param name="tenantId"></param>
        public async Task<ApiResponse<IList<NetworkRecordSummaryResponse>>> GetAllCoreNetworkRecordsAsync(Guid tenantId)
        {
            try
            {
                _logger.LogDebug("[GetAllCoreNetworkRecordsAsync] - Getting all core booking network records for tenantId: {tenantId}", tenantId);

                var items = new List<NetworkRecordSummaryResponse>();

                var httpClient = _httpClientFactory.CreateClient("CoreApiClient");
                httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
                var url = _coreSettings.CoreServiceUri + "bookings/all";
                var response = await httpClient.GetAsync(url);

                if (response.IsSuccessStatusCode)
                {
                    var stream = await response.Content.ReadAsStreamAsync();
                    var lookupResult = Common.Helpers.StreamHelper.DeserializeJsonFromStream<IList<NetworkRecordSummaryResponse>>(stream);
                    if(lookupResult == null)
                    {
                        _logger.LogError("[GetAllCoreNetworkRecordsAsync] - Retrieved no network records from core api for tenantId: {tenantId}", tenantId);
                        return new ApiResponse<IList<NetworkRecordSummaryResponse>>() { Success = false, Code = OperationsApiStatusCodes.E83000 };
                    }
                    items = lookupResult.ToList();
                }
                else
                {
                    _logger.LogError("[GetAllCoreNetworkRecordsAsync] - Unsuccessful call to core api for tenantId: {tenantId}", tenantId);
                    return new ApiResponse<IList<NetworkRecordSummaryResponse>>() { Success = false, Code = OperationsApiStatusCodes.E83000 };
                }

                _logger.LogDebug("[GetAllCoreNetworkRecordsAsync] - Successfully retrieved all core booking network records for tenantId: {tenantId}", tenantId);
                return new ApiResponse<IList<NetworkRecordSummaryResponse>>() { Success = true, Result = items };
            }
            catch (Exception exception)
            {
                _logger.LogError("[GetAllCoreNetworkRecordsAsync] - Unexpected exception: {exception}", exception);
                return new ApiResponse<IList<NetworkRecordSummaryResponse>>() { Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        public async Task<ApiResponse<bool>> UpdateBookingEarlyEndAsync(string bookingReference, BookingEndEarlyDto bookingEndEarlyDto, Guid tenantId)
        {
            try
            {
                _logger.LogDebug("[UpdateBookingEarlyEndAsync] - Begin update booking end date for booking ref: {bookingReference} and tenantid: {tenantId}...", bookingReference, tenantId);

                var existingBooking = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference, tenantId);
                if (existingBooking == null)
                {
                    _logger.LogWarning("[UpdateBookingEarlyEndAsync] - No booking found with reference: {bookingReference}", bookingReference);
                    return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83850 };
                }

                var account = await _accountRepository.GetByIdAsync(existingBooking.AccountId, tenantId, true);
                if (account == null)
                {
                    return new ApiResponse<bool>() { Code = OperationsApiStatusCodes.E83928 };
                }

                var bufferOverride = false;
                var bufferOverrideValue = 0;
                int? currentBufferLength = await _operationsBookingManager.GetCurrentBufferLengthInHoursAsync(bookingReference, tenantId);

                if (bookingEndEarlyDto.EndEarlyReasonId != null)
                {
                    var earlyEndReason = await _earlyEndBookingReasonRepository.GetByIdAsync(bookingEndEarlyDto.EndEarlyReasonId.Value);
                    if (earlyEndReason == null)
                    {
                        _logger.LogWarning("[UpdateBookingEarlyEndAsync] - No early end reason found with id: {earlyEndReasonId}", bookingEndEarlyDto.EndEarlyReasonId);
                        return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83850 };
                    }

                    _logger.LogDebug("[UpdateBookingEarlyEndAsync] - Setting buffer override of {bufferOverrideValue} based on early end reason id: {earlyEndReasonId} for booking reference: {bookingReference}, tenantId: {tenantId}", bufferOverrideValue, bookingEndEarlyDto.EndEarlyReasonId, bookingReference, tenantId);
                    bufferOverride = true;
                    bufferOverrideValue = earlyEndReason.BufferValueOverride;
                }

                if (bookingEndEarlyDto.EndDate >= existingBooking.EndDate)
                {
                    _logger.LogWarning("[UpdateBookingEarlyEndAsync] - Unable to update booking end date to a date greater than current end date. Booking reference: {bookingReference}", bookingReference);
                    return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83850 };
                }

                if (bookingEndEarlyDto.EndDate <= existingBooking.StartDate)
                {
                    _logger.LogWarning("[UpdateBookingEarlyEndAsync] - Unable to update booking end date to a date less than or equal to booking start date. Booking reference: {bookingReference}", bookingReference);
                    return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83850 };
                }

                if (bookingEndEarlyDto.NewOpsStatusId != null && bookingEndEarlyDto.NewOpsStatusId != existingBooking.BookingOpsStatusId)
                {
                    _logger.LogDebug("[UpdateBookingEarlyEndAsync] - Ops status change required during early end for Booking reference: {bookingReference}, tenantID: {tenantId}, new ops status id: {opsStatusId}", bookingReference, tenantId, bookingEndEarlyDto.NewOpsStatusId);

                    var opsStatusesForBookingStatus = await _bookingOpsStatusRepository.GetByBookingStatusAsync((Common.Enums.BookingStatus)existingBooking.BookingStatusId);
                    if (!opsStatusesForBookingStatus.Any(o => o.Id == bookingEndEarlyDto.NewOpsStatusId))
                    {
                        _logger.LogWarning("[UpdateBookingEarlyEndAsync] - Booking ops status with id: {bookingOpsStatusId} not available for booking ref: {bookingReference} and tenantId: {tenantId}", bookingEndEarlyDto.NewOpsStatusId, bookingReference, tenantId);
                        return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E84060 };
                    }

                    var newOpsStatus = opsStatusesForBookingStatus.Single(o => o.Id == bookingEndEarlyDto.NewOpsStatusId);
                    if (newOpsStatus.BufferOverrideEnabled)
                    {
                        _logger.LogDebug("[UpdateBookingEarlyEndAsync] - Setting buffer override of {bufferOverrideValue} based on new ops status id: {newOpsStatusId} for booking reference: {bookingReference}, tenantId: {tenantId}", bufferOverrideValue, bookingEndEarlyDto.NewOpsStatusId, bookingReference, tenantId);
                        bufferOverride = true;
                        bufferOverrideValue = newOpsStatus.BufferOverride;
                    }

                    existingBooking.BookingOpsStatusId = bookingEndEarlyDto.NewOpsStatusId;
                }

                var assetId = existingBooking.AssetId;
                var assetMake = existingBooking.AssetMake;
                var assetModel = existingBooking.AssetModel;
                var assetDescription = existingBooking.AssetDescription;
                var assetReference = existingBooking.AssetReference;
                var bookingStatusId = existingBooking.BookingStatusId;
                var assetImageUrl = existingBooking.AssetImageUrl;
                var bookingTypeId = existingBooking.BookingTypeId;

                //Once CoreV1 architecture is phased out this if block will not be needed.
                if (_coreSettings.CoreV2Enabled)
                {
                    _logger.LogDebug("[UpdateBookingEarlyEndAsync] - CoreV2 enabled so skipping core booking update via HTTP...");
                }
                else
                {
                    _logger.LogDebug("[UpdateBookingEarlyEndAsync] - CoreV2 disabled so updating booking in core via HTTP...");

                    //Update core booking and change buffer based on early end reason               
                    var updatedBookingOrRequest = new UpdatedBookingOrRequestDto()
                    {
                        BufferValue = bufferOverride ? bufferOverrideValue : currentBufferLength,
                        StartDateTime = existingBooking.StartDate,
                        AssetId = existingBooking.AssetId,
                        EndDateTime = bookingEndEarlyDto.EndDate,
                        BookingId = existingBooking.Id
                    };

                    var coreResponse = await UpdateCoreBookingRequestAsync(bookingReference, updatedBookingOrRequest, tenantId);
                    _logger.LogDebug("[UpdateBookingEarlyEndAsync] - Response from core API when updating booking: {bookingReference}. Response: {response}", bookingReference, coreResponse);

                    if (!coreResponse.IsSuccessStatusCode)
                    {
                        _logger.LogWarning("[UpdateBookingEarlyEndAsync] - Response from core API when updating booking: {bookingReference} was not successfull");
                        return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83850 };
                    }

                    var responseContent = await coreResponse.Content.ReadAsStringAsync();
                    var apiResponse = JsonConvert.DeserializeObject<ApiResponse<UpdatedBookingResponse>>(responseContent);
                    var updatedBookingResponse = apiResponse.Result;

                    assetId = updatedBookingResponse.AssetId;
                    assetMake = updatedBookingResponse.AssetMake;
                    assetModel = updatedBookingResponse.AssetModel;
                    assetDescription = updatedBookingResponse.AssetDescription;
                    assetReference = updatedBookingResponse.AssetReference;
                    bookingStatusId = (int)updatedBookingResponse.BookingStatus;
                    assetImageUrl = updatedBookingResponse.AssetImageUrl;
                    bookingTypeId = (int)updatedBookingResponse.BookingType;
                }

                //Update ops booking
                _logger.LogDebug("[UpdateBookingEarlyEndAsync] - Updating operations booking: {bookingReference}...", bookingReference);

                await _operationsBookingManager.UpdateOperationsBookingSummaryRecordAsync(bookingReference, bookingStatusId, tenantId, assetId, assetMake, assetModel,
                    assetReference, assetDescription, assetImageUrl, existingBooking.StartDate, bookingEndEarlyDto.EndDate,
                    bookingTypeId, 
                    bufferOverride ? bufferOverrideValue : currentBufferLength, 
                    false, bookingEndEarlyDto.EndEarlyReasonId);

                var result = await CompleteUnitOfWork();
                await _tagEntityManagementService.CheckAndAddEntityTagAsync(Tags.BOOKING_EARLY_END, tenantId, existingBooking.Id, Models.Enums.EntityType.Booking);

                await _eventQueue.BookingEarlyEndedAsync(new BookingEventData
                {
                    AccountId = existingBooking.AccountId,
                    BookingRef = existingBooking.UniqueReference,
                    DriverId = existingBooking.DriverId,
                    BookingId = existingBooking.Id,
                    Url = GetBackOfficeCustomerUri(existingBooking.AccountId),
                    TenantId = tenantId
                });

                if (_coreSettings.CoreV2Enabled)
                {
                    await _coreV2MessageQueueClient.UpdateBookingAsync(new UpdateBookingMessageData()
                    {
                        BufferValue = bufferOverride ? bufferOverrideValue : currentBufferLength,
                        StartDate = existingBooking.StartDate,
                        AssetId = existingBooking.AssetId,
                        EndDate = bookingEndEarlyDto.EndDate,
                        BookingId = existingBooking.Id,
                        BookingRef = existingBooking.UniqueReference,
                        TenantId = tenantId,
                        AccountId = account.Id,
                        DriverId = existingBooking.DriverId
                    }, tenantId);
                }

                _logger.LogDebug("[UpdateBookingEarlyEndAsync] - Early end booking successfull for booking ref: {bookingReference}", bookingReference);

                return new ApiResponse<bool>() { Success = result, Result = result, Code = OperationsApiStatusCodes.S83860 };
            }
            catch (Exception ex)
            {
                _logger.LogError("[UpdateBookingEarlyEndAsync] - Unexpected exception: {exception}", ex);

                return new ApiResponse<bool>()
                {
                    Success = false,
                    Result = false,
                    Code = OperationsApiStatusCodes.E83851
                };
            }
        }

        public async Task<ApiResponse<bool>> UpdateBookingExtendAsync(string bookingReference, BookingExtendDto bookingExtendDto, Guid tenantId)
        {
            try
            {
                _logger.LogDebug("[UpdateBookingExtendAsync] - Begin update booking end date for booking ref: {bookingReference} and tenantid: {tenantId}...", bookingReference, tenantId);

                var existingBooking = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference, tenantId);
                if (existingBooking == null)
                {
                    _logger.LogWarning("[UpdateBookingExtendAsync] - No booking found with reference: {bookingReference} and tenantId: {tenantId}", bookingReference, tenantId);
                    return ApiResponseHelper.Failure<bool>(OperationsApiStatusCodes.E83860);
                }

                var existingBookingEndDate = existingBooking.EndDate;

                if (bookingExtendDto.EndDate <= existingBooking.EndDate)
                {
                    _logger.LogWarning("[UpdateBookingExtendAsync] - Unable to extend booking end date to a date less than current end date. Booking reference: {bookingReference}, tenantId: {tenantId}", bookingReference, tenantId);
                    return ApiResponseHelper.Failure<bool>(OperationsApiStatusCodes.E84080);
                }

                // We'll use the current buffer length to update the ops and core buffer enddate. This takes into account any previous ops status changes
                // which may have already overridden the default buffer value. If not buffer length will still be the default length so this will be used.
                int? currentBufferLength = await _operationsBookingManager.GetCurrentBufferLengthInHoursAsync(bookingReference, tenantId);

                var assetId = existingBooking.AssetId;
                var assetMake = existingBooking.AssetMake;
                var assetModel = existingBooking.AssetModel;
                var assetDescription = existingBooking.AssetDescription;
                var assetReference = existingBooking.AssetReference;
                var bookingStatusId = existingBooking.BookingStatusId;
                var assetImageUrl = existingBooking.AssetImageUrl;
                var bookingTypeId = existingBooking.BookingTypeId;

                //Once CoreV1 architecture is phased out this if block will not be needed.
                if (_coreSettings.CoreV2Enabled)
                {
                    _logger.LogDebug("[UpdateBookingExtendAsync] - CoreV2 enabled so skipping core booking update via HTTP...");

                    var tenant = await _tenantRepository.GetByIdAsync(existingBooking.TenantId);
                    if (tenant == null)
                    {
                        _logger.LogError("[UpdateBookingExtendAsync] - No tenant found with id: {tenantID}, unable to extend booking", existingBooking.TenantId);
                        return ApiResponseHelper.Failure<bool>(OperationsApiStatusCodes.E84122);
                    }

                    if (!await _bookingRepository.IsAssetAvailableAsync(existingBooking.StartDate, bookingExtendDto.EndDate.AddHours((int)currentBufferLength), assetId, bookingReference))
                    {
                        _logger.LogError("[UpdateBookingExtendAsync] - unable to extend booking, asset with id: {assetId} not available", existingBooking.TenantId);
                        return ApiResponseHelper.Failure<bool>(OperationsApiStatusCodes.E84130);
                    }
                }
                else
                {
                    _logger.LogDebug("[UpdateBookingExtendAsync] - CoreV2 disabled so updating booking in core via HTTP...");
             
                    var updatedBookingOrRequest = new UpdatedBookingOrRequestDto()
                    {
                        BufferValue = currentBufferLength,
                        StartDateTime = existingBooking.StartDate,
                        AssetId = existingBooking.AssetId,
                        EndDateTime = bookingExtendDto.EndDate,
                        BookingId = existingBooking.Id
                    };

                    var coreResponse = await UpdateCoreBookingRequestAsync(bookingReference, updatedBookingOrRequest, tenantId);
                    _logger.LogDebug("[UpdateBookingExtendAsync] - Response from core API when updating booking: {bookingReference}. Response: {response}", bookingReference, coreResponse);

                    if (!coreResponse.IsSuccessStatusCode)
                    {
                        _logger.LogWarning("[UpdateBookingExtendAsync] - Response from core API when updating booking: {bookingReference} was not successfull");
                        return ApiResponseHelper.Failure<bool>(OperationsApiStatusCodes.E84081);
                    }

                    var responseContent = await coreResponse.Content.ReadAsStringAsync();
                    var apiResponse = JsonConvert.DeserializeObject<ApiResponse<UpdatedBookingResponse>>(responseContent);
                    var updatedBookingResponse = apiResponse.Result;

                    assetId = updatedBookingResponse.AssetId;
                    assetMake = updatedBookingResponse.AssetMake;
                    assetModel = updatedBookingResponse.AssetModel;
                    assetDescription = updatedBookingResponse.AssetDescription;
                    assetReference = updatedBookingResponse.AssetReference;
                    bookingStatusId = (int)updatedBookingResponse.BookingStatus;
                    assetImageUrl = updatedBookingResponse.AssetImageUrl;
                    bookingTypeId = (int)updatedBookingResponse.BookingType;
                }

                //Update ops booking
                _logger.LogDebug("[UpdateBookingExtendAsync] - Updating operations booking: {bookingReference}...", bookingReference);

                await _operationsBookingManager.UpdateOperationsBookingSummaryRecordAsync(bookingReference, bookingStatusId, tenantId, assetId, assetMake, assetModel,
                     assetReference, assetDescription, assetImageUrl, existingBooking.StartDate, bookingExtendDto.EndDate,
                     bookingTypeId, currentBufferLength, false);

                var result = await CompleteUnitOfWork();

                _logger.LogDebug("[UpdateBookingExtendAsync] - Extend booking end date successfull for booking ref: {bookingReference}, tenantId: {tenantId}", bookingReference, tenantId);

                var currentUser = _userContext.GetUserName();
                await _eventQueue.AddAuditLogAsync(new Queues.Models.CommandMessageData.AuditCommandData()
                {
                    TenantId = tenantId,
                    AuditEventActionType = Common.Enums.EventActionType.BookingExtended,
                    BookingId = existingBooking.Id,
                    BookingRef = existingBooking.UniqueReference,
                    Author = String.IsNullOrEmpty(currentUser) ? AuditLog.Authors.SYSTEM : currentUser,
                    PropertyChanges = new List<Queues.Models.CommandMessageData.AuditCommandPropertyChange>() {
                        new Queues.Models.CommandMessageData.AuditCommandPropertyChange() {
                            DisplayName = AuditLog.Fields.BOOKING_ENDDATE,
                            ValueBefore = existingBookingEndDate.ToString(),
                            ValueAfter = bookingExtendDto.EndDate.ToString()
                        }
                    }
                });

                await _tagEntityManagementService.CheckAndAddEntityTagAsync(Tags.BOOKING_EXTENDED, tenantId, existingBooking.Id, Models.Enums.EntityType.Booking);
                await _eventQueue.BookingExtendedEventAsync(new BookingEventData()
                {
                    AccountId = existingBooking.AccountId,
                    DriverId = existingBooking.DriverId,
                    BookingRef = existingBooking.UniqueReference,
                    AssetId = existingBooking.AssetId,
                    TenantId = tenantId,
                    Url = GetBackOfficeBookingUri(existingBooking.UniqueReference),
                    BookingId = existingBooking.Id
                });

                if (_coreSettings.CoreV2Enabled)
                {
                    await _coreV2MessageQueueClient.UpdateBookingAsync(new UpdateBookingMessageData()
                    {
                        BufferValue = (int)currentBufferLength,
                        StartDate = existingBooking.StartDate,
                        AssetId = existingBooking.AssetId,
                        EndDate = bookingExtendDto.EndDate,
                        BookingId = existingBooking.Id,
                        BookingRef = existingBooking.UniqueReference,
                        TenantId = tenantId,
                        AccountId = existingBooking.AccountId,
                        DriverId = existingBooking.DriverId
                    }, tenantId);
                }

                return new ApiResponse<bool>() { Success = true, Result = true };
            }
            catch (Exception exception)
            {
                _logger.LogError("[UpdateBookingExtendAsync] - Unexpected exception: {exception}", exception);
                return ApiResponseHelper.Failure<bool>(OperationsApiStatusCodes.E83000);
            }
        }

        public async Task<ApiResponse<Guid>> UpdateBookingExtraFieldsAsync(string bookingReference, BookingExtraFieldDto bookingExtraFieldDto, Guid tenantId)
        {
            try
            {
                _logger.LogInformation("[UpdateBookingExtraFieldsAsync] - Begin update booking extra fields for booking ref: {bookingReference} and tenantid: {tenantId}...", bookingReference, tenantId);

                var existingBooking = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference, tenantId);
                if (existingBooking == null)
                {
                    _logger.LogWarning("[UpdateBookingExtraFieldsAsync] - No booking found with reference: {bookingReference} and tenantId: {tenantId}", bookingReference, tenantId);
                    return new ApiResponse<Guid>() { Success = false, Result = Guid.Empty, Code = OperationsApiStatusCodes.E83860 };
                }

                // If an id is supplied make sure it matches the existing record and if so, update the fields.
                if (bookingExtraFieldDto.BookingId.HasValue)
                {
                    _logger.LogInformation("[UpdateBookingExtraFieldsAsync] - Updating existing extra fields for booking ref: {bookingReference} and tenantid: {tenantId}...", bookingReference, tenantId);

                    if (bookingExtraFieldDto.BookingId.Value != existingBooking.Id)
                    {
                        _logger.LogWarning("[UpdateBookingExtraFieldsAsync] - BookingId: {bookingId} does not match extra fields bookingId: {efBookingId} for tenantId: {tenantId}",
                            existingBooking.Id,
                            bookingExtraFieldDto.BookingId.Value,
                            tenantId
                        );
                        return new ApiResponse<Guid>() { Success = false, Result = Guid.Empty, Code = OperationsApiStatusCodes.E84083 };
                    }
                }
                else
                {
                    _logger.LogInformation("[UpdateBookingExtraFieldsAsync] - Creating new extra fields entry for booking ref: {bookingReference} and tenantid: {tenantId}...", bookingReference, tenantId);

                    existingBooking.ExtraFields = new BookingExtraField()
                    {
                        BookingId = existingBooking.Id,
                        CreatedDate = DateTime.UtcNow
                    };
                }

                existingBooking.ExtraFields.RecurringWeeklyPaymentDay = bookingExtraFieldDto.RecurringWeeklyPaymentDay;
                existingBooking.ExtraFields.TotalCAFApplied = bookingExtraFieldDto.TotalCAFApplied;
                existingBooking.ExtraFields.TotalCAFFundAvailable = bookingExtraFieldDto.TotalCAFFundAvailable;
                existingBooking.ExtraFields.WeeklyCAFContribution = bookingExtraFieldDto.WeeklyCAFContribution;
                existingBooking.ExtraFields.WeeklyTotalInclCAFContribution = bookingExtraFieldDto.WeeklyTotalInclCAFContribution;

                var result = await CompleteUnitOfWork();
                if (!result)
                {
                    _logger.LogWarning("[UpdateBookingExtraFieldsAsync] -Unable to save extra field changes for bookingRef: {ref} and tenantId: {tenantId}",
                            bookingReference,
                            tenantId
                        );
                    return new ApiResponse<Guid>() { Success = false, Result = Guid.Empty, Code = OperationsApiStatusCodes.E84084 };
                }

                _logger.LogInformation("[UpdateBookingExtraFieldsAsync] - Extra fields updated successfully for booking ref: {bookingReference}, tenantId: {tenantId}", bookingReference, tenantId);

                return new ApiResponse<Guid>() { Success = true, Result = existingBooking.Id };
            }
            catch (Exception exception)
            {
                _logger.LogError("[UpdateBookingExtraFieldsAsync] - Unexpected exception: {exception}", exception);
                return new ApiResponse<Guid> { Success = false, Code = OperationsApiStatusCodes.E83000, Result = Guid.Empty };
            }
        }

        public async Task<ApiResponse<bool>> RenewBookingAsync(string bookingReference, BookingRenewalDto bookingRenewalDto, Guid tenantId, Guid globalTenantId)
        {
            var existingBooking = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference, tenantId);
            if (existingBooking == null)
            {
                _logger.LogWarning("[RenewBookingAsync] - No booking found with reference: {bookingReference} and tenantId: {tenantId}", bookingReference, tenantId);
                return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83860 };
            }

            var account = await _accountRepository.GetByIdAsync(existingBooking.AccountId, tenantId);
            if (account == null)
            {
                return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E83928 };
            }

            var assetPricing = await _assetPackagePricingRepository.GetByIdAsync(bookingRenewalDto.AssetPackagePricingId, tenantId);
            if (assetPricing == null)
            {
                //check to see if the pricing and package information belongs to the global tenant
                if (globalTenantId != Guid.Empty)
                {
                    assetPricing = await _assetPackagePricingRepository.GetByIdAsync(bookingRenewalDto.AssetPackagePricingId, globalTenantId);
                }
            }

            if (assetPricing == null)
            {
                _logger.LogWarning("[RenewBookingAsync] - Asset package pricing with id: {assetPackagePricingId} for tenantId: {tenantId} or globalTenantId: {globalTenantId} does not exist", bookingRenewalDto.AssetPackagePricingId, tenantId, globalTenantId);
                return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E83861 };
            }

            var bookingExtraOptions = new List<ExtraOption>();
            if (bookingRenewalDto.BookingExtras != null && bookingRenewalDto.BookingExtras.Any())
            {
                var extraOptions = await _extraOptionRepository.GetByIdsAsync(tenantId, bookingRenewalDto.BookingExtras.Select(e => e.ExtraOptionId), true);
                if (extraOptions.Count() != bookingRenewalDto.BookingExtras.Count())
                {
                    _logger.LogWarning("[RenewBookingAsync] - Unable to find one or more extraoptions specified in request payload for AccountId: {accountId}, TenantId: {tenantId}", account.Id, tenantId);
                    return new ApiResponse<bool>() { Success = false, Code = "404" };
                }
                bookingExtraOptions = extraOptions.ToList();
            }

            var newBookingStartDate = existingBooking.EndDate;

            var durationUnit = existingBooking.DurationUnit;
            var newBookingEndDate = DurationHelper.CalculateEndDateFromDuration(assetPricing.DurationInHours, durationUnit, newBookingStartDate, _jrnyOperationsSettings.LocalRegionTimeZoneId);
            _logger.LogTrace("[RenewBookingAsync] - Booking end date calculated as: {bookingEndDate} based on a start date of {bookingStartDate}, duration hours: {durationHours}, duration unit: {durationUnit}. AccountId: {accountId}, TenantId: {tenantId}", newBookingEndDate, newBookingStartDate, assetPricing.DurationInHours, durationUnit, account.Id, tenantId);

            var httpClient = _httpClientFactory.CreateClient("CoreApiClient");

            if (_coreSettings.CoreV2Enabled)
            {
                _logger.LogDebug("[RenewBookingAsync] - CoreV2 enabled so checking asset availability for renweal from ops..");

                var tenant = await _tenantRepository.GetByIdAsync(tenantId);
                if (tenant == null)
                {
                    _logger.LogError("[RenewBookingAsync] - No tenant found with id: {tenantID}, unable to renew booking", existingBooking.TenantId);
                    return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E84122 };
                }

                var buffer = tenant.BufferValue;
                var endDateWithBuffer = newBookingEndDate.AddHours(buffer);

                _logger.LogDebug("[RenewBookingAsync] - Checking asset with id {assetId} is available between {startDate} and" +
                    " {endDate} including a buffer length of {bufferLength} hours", existingBooking.AssetId, newBookingStartDate,
                    endDateWithBuffer, tenant.BufferValue);

                var available = await _bookingRepository.IsAssetAvailableAsync(newBookingStartDate, endDateWithBuffer, existingBooking.AssetId, existingBooking.UniqueReference);
                if (!available)
                {
                    _logger.LogWarning("[RenewBookingAsync] - Asset not available for renewal for booking ref: {bookingReference}", bookingReference);
                    return new ApiResponse<bool>()
                    {
                        Success = false,
                        Result = false,
                        Code = OperationsApiStatusCodes.E83862
                    };
                }
            }
            else
            {
                //Check asset is available for new booking (will need to ignore buffer records)
                httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
                var url = $"{httpClient.BaseAddress}assets/{existingBooking.AssetId}/bookings";

                var response = await httpClient.GetAsync(url);
                var responseContent = await response.Content.ReadAsStringAsync();
                var apiResponse = JsonConvert.DeserializeObject<IList<NetworkRecordSummaryResponse>>(responseContent);
                var clashesIncBufferRecords = apiResponse.Where(nr => nr.StartDate >= existingBooking.EndDate && nr.StartDate <= newBookingEndDate);

                if (clashesIncBufferRecords.Any(nr => nr.NetworkRecordType != (int)Common.Enums.NetworkRecordType.Buffer))
                {
                    _logger.LogWarning("[RenewBookingAsync] - Asset not available for renewal for booking ref: {bookingReference}", bookingReference);
                    return new ApiResponse<bool>()
                    {
                        Success = false,
                        Result = false,
                        Code = OperationsApiStatusCodes.E83862
                    };
                }
            }

            //Construct new booking for renewal                            
            var newBookingRequest = new NewBookingOrRequest()
            {
                StartDate = newBookingStartDate,
                EstimatedEndDate = newBookingEndDate,
                StartLongitude = 0.0,
                StartLatitude = 0.0,
                EstimatedReturnLongitude = 0.0,
                EstimatedReturnLatitude = 0.0,
                AssetId = existingBooking.AssetId,
                ExternalId = existingBooking.DriverId.ToString(),
                DrivingLicenceNumber = existingBooking.Driver.DrivingLicence ?? existingBooking.DriverId.ToString(),
                DriverName = $"{existingBooking.Driver.Title} {existingBooking.Driver.Firstname} {existingBooking.Driver.Surname}",
                DriverTelephoneNumber = existingBooking.Driver.MobileTelephone,
                OtherData = "",
                ExternalReference = "",
                Notes = bookingRenewalDto.Notes,
                UsageTypeId = Guid.Empty
            };

            var assetMake = "";
            var assetModel = "";
            var assetVariant = "";
            var newBookingReference = "";
            var assetReference = "";
            var tenantName = "";
            var assetImageUrl = "";

            //Once CoreV1 architecture is phased out this if block will not be needed.
            if (_coreSettings.CoreV2Enabled)
            {
                _logger.LogDebug("[RenewBookingAsync] - CoreV2 enabled so skipping core booking renewal via HTTP...");

                var opsAsset = await _assetRepository.GetByIdAsync(existingBooking.AssetId);
                if (opsAsset == null)
                {
                    _logger.LogError("[RenewBookingAsync] - Unable to renew booking, no asset in ops with id {assetId}", existingBooking.AssetId);
                    return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E84121 };
                }

                var bookingTenant = await _tenantRepository.GetByIdAsync(tenantId);
                if (opsAsset == null)
                {
                    _logger.LogError("[RenewBookingAsync] - Unable to renew booking, no booking tenant in ops with id {tenantId}", tenantId);
                    return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E84122 };
                }

                var assetOwnerTenant = await _tenantRepository.GetByIdAsync(opsAsset.TenantId);
                if (opsAsset == null)
                {
                    _logger.LogError("[RenewBookingAsync] - Unable to renew booking, no asset owner tenant in ops with id {tenantId}", opsAsset.TenantId);
                    return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E84122 };
                }

                // Load existing bookings using the booking reference prefix to work out the new reference.
                var bookingRefPrefix = bookingReference.Split('_')[0];
                var existingBookings = await _bookingRepository.GetAllBookingsOrRequestsByBookingReferencePrefixAsync(bookingRefPrefix, tenantId);
                var latestBooking = existingBookings.OrderByDescending(b => b.CreatedDate).First();

                assetMake = opsAsset.Make;
                assetModel = opsAsset.Model;
                assetVariant = opsAsset.Variant;
                assetReference = opsAsset.Identifier;
                tenantName = bookingTenant.Label;
                assetImageUrl = opsAsset.PropertyValues.FirstOrDefault(a => a.Master_AssetPropertyId == Guid.NewGuid())?.Value; //TODO: load correct property
                newBookingReference = BookingReferenceHelper.GetIncrementedBookingReference(latestBooking.UniqueReference);
            }
            else
            {
                string json = JsonConvert.SerializeObject(newBookingRequest, Formatting.None);
                _logger.LogDebug("[RenewBookingAsync] - Renewing booking ref: {bookingReference}...", bookingReference);
                var url = $"{httpClient.BaseAddress}booking/{existingBooking.UniqueReference}/renewal";
                var response = await httpClient.PostAsync(url, new StringContent(json, System.Text.Encoding.UTF8, "application/json"));

                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogError("[RenewBookingAsync] - Response from core when attempting to renew booking ref: {bookingReference} not success", bookingReference);
                    return new ApiResponse<bool>()
                    {
                        Success = false,
                        Result = false,
                        Code = OperationsApiStatusCodes.E83863
                    };
                }

                var responseContent = await response.Content.ReadAsStringAsync();
                var renewApiResponse = JsonConvert.DeserializeObject<ApiResponse<common.Responses.NewBookingResponse>>(responseContent);
                var newBookingResponse = renewApiResponse.Result;

                if (renewApiResponse.Result == null)
                {
                    return new ApiResponse<bool>()
                    {
                        Success = false,
                        Result = false,
                        Code = OperationsApiStatusCodes.E83863
                    };
                }

                assetMake = newBookingResponse.AssetMake;
                assetModel = newBookingResponse.AssetModel;
                assetVariant = newBookingResponse.AssetDescription;
                newBookingReference = newBookingResponse.BookingReference;
                assetReference = newBookingResponse.AssetReference;
                tenantName = newBookingResponse.ProviderLabel;
                assetImageUrl = newBookingResponse.AssetImageUrl;
            }

            var discountAndVouchers = await GetVouchersAndDiscountedPriceAsync(bookingRenewalDto.VoucherCodes, assetPricing, tenantId, globalTenantId);
            _logger.LogDebug("[RenewBookingAsync] - Price for new booking is: {price} and discounted price with voucher codes is: {discountedPrice}. AccountId: {accountId}, DriverId: {driverId}, TenantId: {tenantId}", assetPricing.Price, discountAndVouchers.Item1, existingBooking.Id, existingBooking.Id, tenantId);

            if (bookingRenewalDto.VoucherCodes != null && bookingRenewalDto.VoucherCodes.Any())
            {
                //If vouchers submitted are no longer valid then reject bookingrequest
                if (bookingRenewalDto.VoucherCodes.Count() != discountAndVouchers.Item2.Count)
                {
                    _logger.LogWarning("[RenewBookingAsync] - Valid vouchers did not match vouchers submitted on new booking. AccountId: {accountId}, DriverId: {driverId}, TenantId: {tenantId}", existingBooking.Id, existingBooking.Id, tenantId);
                    return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E83709 };
                }
            }

            var populatedBookingExtras = await _operationsBookingManager.PopulateBookingExtrasAsync(
                bookingRenewalDto.BookingExtras,
                bookingExtraOptions,
                tenantId,
                assetMake,
                assetModel,
                assetVariant
            );

            //Re-take snapshot of asset pricing based on latest
            var assetPricingSnapshot = await _operationsBookingManager.CreateAssetPricingSnapshot(assetMake, assetModel, assetVariant, tenantId, assetPricing.Mileage);

            // Is there a buffer rec to cancel for the booking we're renewing?
            var bufferRec = await _bookingRepository.GetByBookingReferenceAndTypeAsync(bookingReference, Common.Enums.NetworkRecordType.Buffer, tenantId);
            if (bufferRec != null)
            {
                _logger.LogInformation("[RenewBookingAsync] - Cancelling existing buffer record for bookingRef: {ref} and tenantId: {tenantId}", bookingReference, tenantId);
                bufferRec.BookingStatusId = (int)Common.Enums.BookingStatus.Cancelled;
            }

            var bookingId = await _operationsBookingManager.CreateOperationsBookingSummaryRecordAsync(existingBooking.AssetId,
                newBookingReference,
                assetMake,
                assetModel,
                assetVariant,
                assetReference,
                assetImageUrl,
                tenantName,
                newBookingStartDate,
                newBookingEndDate,
                Common.Enums.NetworkRecordType.Booking,
                existingBooking.DriverId,
                existingBooking.AccountId,
                bookingRenewalDto.Notes,
                existingBooking.DeliveryAddress,
                existingBooking.CollectionAddress,
                newBookingRequest.StartDate,
                newBookingRequest.EstimatedEndDate,
                assetPricing,
                discountAndVouchers.Item1,
                existingBooking.DurationUnit,
                tenantId,
                populatedBookingExtras,
                assetPricingSnapshot,
                createBufferRecord: true);

            _logger.LogDebug("[RenewBookingAsync] - Creating basket for booking with id: {bookingId} and tenantId: {tenantId}...", bookingId, tenantId);

            var basketItems = _basketManager.CreateBasketItemsForBooking(assetPricing, discountAndVouchers, populatedBookingExtras);
            await _basketManager.AddBookingBasketAsync(bookingId, basketItems, tenantId);


            var result = await CompleteUnitOfWork();
            await IncrementAndSnapshotVouchers(tenantId, discountAndVouchers, newBookingReference, result);
            await CheckForReferralVoucherOnBookingAsync(discountAndVouchers.Item2, tenantId, globalTenantId);

            if (_coreSettings.CoreV2Enabled)
            {
                // Queue up new core message for renew
                await _coreV2MessageQueueClient.RenewBookingAsync(new NewBookingOrRequestMessageData()
                {
                    AccountId = account.Id,
                    TenantId = tenantId,
                    StartDate = newBookingStartDate,
                    EstimatedEndDate = newBookingEndDate,
                    StartLongitude = 0.0,
                    StartLatitude = 0.0,
                    EstimatedReturnLongitude = 0.0,
                    EstimatedReturnLatitude = 0.0,
                    AssetId = existingBooking.AssetId,
                    ExternalId = existingBooking.DriverId.ToString(),
                    DrivingLicenceNumber = existingBooking.Driver.DrivingLicence ?? existingBooking.Driver.Id.ToString(),
                    DriverName = string.Format("{0} {1} {2}", existingBooking.Driver.Title, existingBooking.Driver.Firstname, existingBooking.Driver.Surname),
                    DriverTelephoneNumber = existingBooking.Driver.MobileTelephone,
                    OtherData = "",
                    ExternalReference = "",
                    Notes = bookingRenewalDto.Notes,
                    UsageTypeId = Guid.Empty,
                    IsVehicleExchange = false,
                    VehicleExchangeExistingUniqueReference = existingBooking.UniqueReference,
                    BookingRef = newBookingReference
                }, tenantId);
            }

            var bookingRecord = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(newBookingReference, tenantId);

            await _tagEntityManagementService.CheckAndAddEntityTagAsync(Tags.BOOKING_RENEWED, tenantId, existingBooking.Id, Models.Enums.EntityType.Booking);
            await _tagEntityManagementService.CheckAndAddEntityTagAsync(Tags.BOOKING_RENEWAL, tenantId, bookingRecord.Id, Models.Enums.EntityType.Booking);

            await _eventQueue.BookingApprovedEventAsync(new BookingEventData
            {
                AccountId = bookingRecord.AccountId,
                DriverId = bookingRecord.DriverId,
                BookingRef = bookingRecord.UniqueReference,
                AssetId = bookingRecord.AssetId,
                TenantId = tenantId,
                Url = GetBackOfficeBookingUri(bookingRecord.UniqueReference),
                BookingId = bookingRecord.Id
            });


            _logger.LogInformation("[RenewBookingAsync] - Renewal successful for booking ref: {bookingReference}", bookingReference);
            return new ApiResponse<bool>()
            {
                Success = true,
                Result = true
            };
        }

        public async Task<ApiResponse<bool>> UpdateBookingOpsStatusAsync(string bookingReference, UpdateBookingOpsStatusDto updateBookingOpsStatusDto, Guid tenantId)
        {
            var booking = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference);

            if (booking == null)
            {
                _logger.LogWarning("[UpdateBookingOpsStatusAsync] - No booking found with bookingReference: {bookingReference} and tenantId: {tenantId}", bookingReference, tenantId);
                return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E83860 };
            }

            return await UpdateBookingOpsStatusCoreAsync(booking, updateBookingOpsStatusDto, tenantId);
        }

        public async Task<ApiResponse<bool>> UpdateBookingOpsStatusAsync(Guid bookingId, UpdateBookingOpsStatusDto updateBookingOpsStatusDto, Guid tenantId)
        {
            var booking = await _bookingRepository.GetByIdAsync(bookingId, tenantId);

            if (booking == null)
            {
                _logger.LogWarning("[UpdateBookingOpsStatusAsync] - No booking found with booking Id: {bookingId} and tenantId: {tenantId}", bookingId, tenantId);
                return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E83860 };
            }

            return await UpdateBookingOpsStatusCoreAsync(booking, updateBookingOpsStatusDto, tenantId);
        }

        public async Task<ApiResponse<bool>> UpdateBookingOpsStatusCoreAsync(Booking booking, UpdateBookingOpsStatusDto updateBookingOpsStatusDto, Guid tenantId)
        {
            try
            {
                var currentBookingOpsStatusLabel = booking.BookingOpsStatus?.Label;
                var currentBookingOpsStatusId = booking.BookingOpsStatusId;
                var currentBookingOpsStatusDescription = booking.BookingOpsStatus?.Label;
                var bookingReference = booking.UniqueReference;

                var opsStatusesForBookingStatus = await _bookingOpsStatusRepository.GetByBookingStatusAsync((Common.Enums.BookingStatus)booking.BookingStatusId);
                if (!opsStatusesForBookingStatus.Any(o => o.Id == updateBookingOpsStatusDto.BookingOpsStatusId))
                {
                    _logger.LogWarning("[UpdateBookingOpsStatusAsync] - Booking ops status with id: {bookingOpsStatusId} not available for booking ref: {bookingReference} and tenantId: {tenantId}", updateBookingOpsStatusDto.BookingOpsStatusId, bookingReference, tenantId);
                    return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E84060 };
                }

                if (booking.BookingOpsStatusId == updateBookingOpsStatusDto.BookingOpsStatusId)
                {
                    _logger.LogDebug("[UpdateBookingOpsStatusAsync] - Booking ref: {bookingReference} already set to ops status with id: {bookingOpsStatusId} for tenantId: {tenantId}. No action required.", bookingReference, updateBookingOpsStatusDto.BookingOpsStatusId, tenantId);
                    return new ApiResponse<bool>() { Result = true, Success = true };
                }

                booking.BookingOpsStatusId = updateBookingOpsStatusDto.BookingOpsStatusId;

                //Update core booking and change buffer based on early end reason               
                var updatedBookingOrRequest = new UpdatedBookingOrRequestDto()
                {
                    StartDateTime = booking.StartDate,
                    AssetId = booking.AssetId,
                    EndDateTime = booking.EndDate,
                    BookingId = booking.Id,
                };

                var newOpsStatus = opsStatusesForBookingStatus.Single(i => i.Id == updateBookingOpsStatusDto.BookingOpsStatusId);
                if (newOpsStatus.BufferOverrideEnabled)
                {
                    _logger.LogDebug("[UpdateBookingOpsStatusAsync] - new ops status of {newOpsStatus} for booking with ref: {bookingRef} has buffer ovveride enabled and override value of {overrideValue}", newOpsStatus.Label, booking.UniqueReference, newOpsStatus.BufferOverride);
                    updatedBookingOrRequest.BufferValue = newOpsStatus.BufferOverride;

                    //Once corev1 phased out then if statement can be removed
                    if (_coreSettings.CoreV2Enabled)
                    {
                        _logger.LogDebug("[UpdateBookingOpsStatusAsync] - CoreV2 enabled so skipping core booking update via HTTP...");
                    }
                    else
                    {
                        _logger.LogDebug("[UpdateBookingOpsStatusAsync] - CoreV2 disabled so updating core booking via HTTP...");
                        var coreResponse = await UpdateCoreBookingRequestAsync(bookingReference, updatedBookingOrRequest, tenantId);
                        _logger.LogDebug("[UpdateBookingOpsStatusAsync] - Response from core API when updating booking: {bookingReference}. Response: {response}", bookingReference, coreResponse);

                        if (!coreResponse.IsSuccessStatusCode)
                        {
                            _logger.LogWarning("[UpdateBookingOpsStatusAsync] - Response from core API when updating booking: {bookingReference} was not successful");
                            return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E84081 };
                        }
                    }

                    // Update the buffer record.
                    var bufferRec = await _bookingRepository.GetByBookingReferenceAndTypeAsync(bookingReference, Common.Enums.NetworkRecordType.Buffer, tenantId);
                    if (bufferRec != null)
                    {
                        _logger.LogInformation("[UpdateBookingOpsStatusAsync] - Found buffer record for bookingRef: {bookingRef} and tenantId: {tenantId}. Updating buffer to {hours} hour(s)",
                            bookingReference,
                            tenantId,
                            newOpsStatus.BufferOverride
                        );
                        bufferRec.EndDate = bufferRec.StartDate.AddHours(newOpsStatus.BufferOverride);
                    }
                    else
                    {
                        _logger.LogWarning("[UpdateBookingOpsStatusAsync] - Unable to find buffer record for bookingRef: {bookingRef} and tenantId: {tenantId}. Unable to update buffer to {hours} hour(s)",
                            bookingReference,
                            tenantId,
                            newOpsStatus.BufferOverride
                       );
                    }
                }

                var result = await CompleteUnitOfWork();
                var currentUser = _userContext.GetUserName();

                if (_coreSettings.CoreV2Enabled && newOpsStatus.BufferOverrideEnabled)
                {
                    _logger.LogDebug("[UpdateBookingOpsStatusAsync] - Raising BookingUpdated event for Booking '{bookingReference}' for tenantId: {tenantId}. No action required.", bookingReference, tenantId);
                    await _coreV2MessageQueueClient.UpdateBookingAsync(new UpdateBookingMessageData()
                    {
                        DriverId = booking.DriverId,
                        StartDate = booking.StartDate,
                        EndDate = booking.EndDate,
                        AccountId = booking.AccountId,
                        AssetId = booking.AssetId,
                        BookingId = booking.Id,
                        BookingRef = booking.UniqueReference,
                        BufferValue = newOpsStatus.BufferOverride,
                        TenantId = booking.TenantId
                    }, tenantId);
                }

                await _eventQueue.BookingOpsStatusChangedEventAsync(new BookingOpsStatusChangedEventData
                {
                    TenantId = booking.TenantId,
                    AccountId = booking.AccountId,
                    AssetId = booking.AssetId,
                    BookingId = booking.Id,
                    BookingRef = booking.UniqueReference,
                    DriverId = booking.DriverId,
                    NewBookingOpsStatus = booking.BookingOpsStatusId.Value, // assume we can not remove a booking Ops Status
                    NewBookingOpsStatusDescription = booking.BookingOpsStatus.Label,
                    OldBookingOpsStatus = currentBookingOpsStatusId,
                    OldBookingOpsStatusDescription = currentBookingOpsStatusLabel
                });

                await _eventQueue.AddAuditLogAsync(new Queues.Models.CommandMessageData.AuditCommandData()
                {
                    TenantId = tenantId,
                    AuditEventActionType = Common.Enums.EventActionType.OpsStatusChanged,
                    BookingId = booking.Id,
                    BookingRef = booking.UniqueReference,
                    Author = String.IsNullOrEmpty(currentUser) ? AuditLog.Authors.SYSTEM : currentUser,
                    PropertyChanges = new List<Queues.Models.CommandMessageData.AuditCommandPropertyChange>() {
                        new Queues.Models.CommandMessageData.AuditCommandPropertyChange() {
                            DisplayName = AuditLog.Fields.OPS_STATUS,
                            ValueBefore = currentBookingOpsStatusLabel == null ? AuditLog.Values.NA : currentBookingOpsStatusLabel,
                            ValueAfter = opsStatusesForBookingStatus.First(o => o.Id == updateBookingOpsStatusDto.BookingOpsStatusId).Label
                        }
                    }
                });

                return new ApiResponse<bool>() { Result = true, Success = true };

            }
            catch (Exception exception)
            {
                _logger.LogError("[UpdateBookingOpsStatusAsync] - Unexpected exception: {exception}", exception);
                return new ApiResponse<bool>() { Success = false, Code = OperationsApiStatusCodes.E83000 };
            }
        }

        private async Task IncrementAndSnapshotVouchers(Guid tenantId, Tuple<decimal, IList<VoucherDto>> discountAndVouchers, string bookingReference, bool result)
        {
            if (result && discountAndVouchers.Item2.Any())
            {
                // Actions if voucher code/s present
                var bookingRecord = await _bookingRepository.GetBookingOrRequestByBookingReferenceAsync(bookingReference, tenantId);

                foreach (var voucher in discountAndVouchers.Item2)
                {
                    await _voucherService.SnapshotVoucherForBookingAsync(voucher, bookingRecord.Id, tenantId);
                    await _voucherService.IncrementUsageCountOfVoucherAsync(voucher, tenantId);
                }
            }
        }

        private async Task CheckForReferralVoucherOnBookingAsync(IList<VoucherDto> vouchers, Guid tenantId, Guid globalTenantId)
        {
            foreach (var voucher in vouchers)
            {
                if (!voucher.Referral)
                {
                    continue;
                }

                Account account = await _voucherService.GetAccountForReferralVoucherAysnc(voucher.Id, tenantId, globalTenantId);

                if (account == null)
                {
                    _logger.LogWarning("[CheckForReferralVoucherOnBookingVouchersAsync] - No associated account was found for referral voucher: {voucherId}", voucher.Id);
                    continue;
                }

                _logger.LogDebug("[CheckForReferralVoucherOnBookingVouchersAsync] - Queuing up ReferralCodeUsedEvent for referralCode: {referralCode}.", voucher.Code);

                var bookings = await _bookingRepository.GetAllBookingsOrRequestsByAccountAsync(account.Id, tenantId);

                // get the current active booking if it exists
                var currentBooking = bookings.SingleOrDefault(x => x.BookingStatusId == (int)BookingStatus.Started || x.BookingStatusId == (int)BookingStatus.Placed);

                if (currentBooking == null)
                {
                    _logger.LogWarning("[CheckForReferralVoucherOnBookingAsync] - No placed or active bookings found for account: {accountId}", account.Id);
                }

                await _eventQueue.ReferralCodeUsedEventAsync(new ReferralCodeUsedEventData()
                {
                    TenantId = tenantId,
                    AccountId = account.Id,
                    ReferralCodeOwnerAccountId = account.Id,
                    Firstname = account.Firstname,
                    Surname = account.Surname,
                    EmailAddress = account.EmailAddress,
                    ReferralCode = voucher.Code,
                    AssetRegistration = currentBooking != null ? currentBooking.AssetReference : "N/a",
                    BookingReference = currentBooking != null ? currentBooking.UniqueReference : "N/a",
                    Url = GetBackOfficeCustomerUri(account.Id)
                });
            }
        }

        private static List<string> ReadVoucherCodesFromDto(NewBookingOrRequestDto newBooking)
        {
            //Support multiple and single voucher code implenation to prevent introducing breaking change
            var voucherCodes = new List<string>();
            if (newBooking.VoucherCodes != null)
            {
                voucherCodes.AddRange(newBooking.VoucherCodes);
            }

            if (!string.IsNullOrEmpty(newBooking.VoucherCode))
            {
                voucherCodes.Add(newBooking.VoucherCode);
            }

            return voucherCodes;
        }

        private async Task<Tuple<decimal, IList<VoucherDto>>> GetVouchersAndDiscountedPriceAsync(IList<string> voucherCodes, AssetPackagePricing assetPricing, Guid tenantId, Guid globalTenantId, bool ignoreUsageLimits = false)
        {
            decimal discountedPrice = 0;
            IList<VoucherDto> vouchers = new List<VoucherDto>();
            if (voucherCodes != null && voucherCodes.Any() && assetPricing != null)
            {
                vouchers = await _voucherService.GetValidVouchersAsync(voucherCodes, assetPricing.Id, tenantId, globalTenantId, ignoreUsageLimits);

                if (vouchers.Count > 1 && vouchers.Any(v => !v.VoucherCategory.UseWithOtherCategorys))
                {
                    _logger.LogWarning("[GetVouchersAndDiscountedPrice] - Unable to apply discount - one or more vouchers specified cannot be used with other voucher categories. TenantId: {tenantId}", tenantId);
                    return new Tuple<decimal, IList<VoucherDto>>(discountedPrice, new List<VoucherDto>());
                }

                // Confirm each voucher has it's own catagory
                if (!_voucherService.IsOnly1VoucherPerCategory(vouchers))
                {
                    _logger.LogWarning("[GetVouchersAndDiscountedPrice] - Unable to apply discount - more than one voucher from a single category specified. TenantId: {tenantId}", tenantId);
                    return new Tuple<decimal, IList<VoucherDto>>(discountedPrice, new List<VoucherDto>());
                }

                discountedPrice = assetPricing.Price;
                foreach (var voucher in vouchers.Where(v => v.VoucherType == Models.Enums.VoucherType.WeeklyDiscount)?.OrderBy(v => v.VoucherCategory.Order))
                {
                    discountedPrice = _voucherManager.ApplyVoucherToPrice(voucher, discountedPrice);
                }

                if (discountedPrice == assetPricing.Price)
                {
                    discountedPrice = 0;
                }
            }

            return new Tuple<decimal, IList<VoucherDto>>(discountedPrice, vouchers);
        }

        private async Task<HttpResponseMessage> UpdateCoreBookingRequestAsync(string bookingReference, UpdatedBookingOrRequestDto updatedBookingOrRequest, Guid tenantId)
        {
            HttpClient httpClient = _httpClientFactory.CreateClient("CoreApiClient");
            httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
            var url = string.Format("{0}{1}{2}", httpClient.BaseAddress, "booking/", bookingReference);

            var updatedBookingRequest = _mapper.Map<common.Requests.UpdatedBookingRequest>(updatedBookingOrRequest);

            string json = JsonConvert.SerializeObject(updatedBookingRequest, Formatting.None);
            var response = await httpClient.PutAsync(url, new StringContent(json, System.Text.Encoding.UTF8, "application/json"));
            return response;
        }

        public async Task<ApiResponse<bool>> ValidateNewVoucherOnBookingAsync(string bookingReference, BookingVoucherCodesDto bookingVoucherCodesDto, Guid tenantId, Guid globalTenantId)
        {
            try
            {
                var booking = await GetBookingDetailsAsync(bookingReference, tenantId);
                List<string> newVoucherCodes = bookingVoucherCodesDto.Codes.Where(x => !booking.Result.BookingVouchers.Any(y => y.Code == x)).ToList();

                // Confirm that the new vouchers found on the booking are valid to be used
                var newlyAddedVouchers = await _voucherService.GetValidVouchersAsync(newVoucherCodes, booking.Result.AssetPackagePricingId, tenantId, globalTenantId);

                if (!newlyAddedVouchers.Any() || newlyAddedVouchers.Count != newVoucherCodes.Count)
                {
                    _logger.LogWarning("[ValidateNewVoucherOnBookingAsync] - New vouchers did not match vouchers submitted. AssetPackagePriceId {assetPackagePricingId} and tenantId: {tenantId}.");
                    return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83709 };
                }

                var assetPricing = await _assetPackagePricingRepository.GetByIdAsync(booking.Result.AssetPackagePricingId, tenantId);
                if (assetPricing == null)
                {
                    //check to see if the pricing and package information belongs to the global tenant
                    if (globalTenantId != Guid.Empty)
                    {
                        assetPricing = await _assetPackagePricingRepository.GetByIdAsync(booking.Result.AssetPackagePricingId, globalTenantId);
                    }
                }

                if (assetPricing == null)
                {
                    return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83703 };
                }

                // Validate all the codes against the package but DON'T run specific code level checks as some may have maxed out their usage when the booking was created.
                var discountAndVouchers = await GetVouchersAndDiscountedPriceAsync(bookingVoucherCodesDto.Codes, assetPricing, tenantId, globalTenantId);

                // Reject if the valid number of vouchers doesn't add up.
                if (bookingVoucherCodesDto.Codes.Count != discountAndVouchers.Item2.Count)
                {
                    _logger.LogWarning("[AddBookingAsync] - Valid vouchers did not match vouchers submitted with booking. TenantId: {tenantId}", tenantId); // sort out logging
                    return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83709 };
                }

                return new ApiResponse<bool>() { Success = true, Result = true, Code = OperationsApiStatusCodes.S83861 };
            }
            catch (Exception exception)
            {
                _logger.LogError("[ValidateNewVoucherOnBookingAsync] - Unexpected exception: {exception}", exception);
                return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83853 };
            }
        }

        public async Task<ApiResponse<bool>> UpdateBookingVouchersAsync(string bookingReference, BookingVoucherCodesDto bookingVoucherCodesDto, Guid tenantId, Guid globalTenantId)
        {
            try
            {
                var booking = await GetBookingDetailsAsync(bookingReference, tenantId);
                List<string> newVoucherCodes = bookingVoucherCodesDto.Codes.Where(x => !booking.Result.BookingVouchers.Any(y => y.Code == x)).ToList();
                List<BookingVoucherDto> removedVouchers = booking.Result.BookingVouchers.Where(x => !bookingVoucherCodesDto.Codes.Any(y => y == x.Code)).ToList();

                // Confirm that the new vouchers found on the booking are valid to be used
                List<VoucherDto> newlyAddedVouchers = (List<VoucherDto>)await _voucherService.GetValidVouchersAsync(newVoucherCodes, booking.Result.AssetPackagePricingId, tenantId, globalTenantId);

                if (!newlyAddedVouchers.Any() && newVoucherCodes.Count > 1 || newlyAddedVouchers.Count != newVoucherCodes.Count)
                {
                    _logger.LogWarning("[UpdateBookingVouchersAsync] - New vouchers did not match vouchers submitted. AssetPackagePriceId {assetPackagePricingId} and tenantId: {tenantId}.");
                    return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83709 };
                }

                // Get the assetPricingPackage on the booking ready for discount calculation.
                var assetPricing = await _assetPackagePricingRepository.GetByIdAsync(booking.Result.AssetPackagePricingId, tenantId);
                if (assetPricing == null)
                {
                    //check to see if the pricing and package information belongs to the global tenant
                    if (globalTenantId != Guid.Empty)
                    {
                        assetPricing = await _assetPackagePricingRepository.GetByIdAsync(booking.Result.AssetPackagePricingId, globalTenantId);
                    }
                }

                if (assetPricing == null)
                {
                    return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83703 };
                }

                // Retrieve the voucher discount whilst validating the new voucherCodes alongside the existing. Only catagory level checks will 
                // be carried out as existing vouchers may have maxed out their usage during the booking process.
                var discountAndVouchers = await GetVouchersAndDiscountedPriceAsync(bookingVoucherCodesDto.Codes, assetPricing, tenantId, globalTenantId, true);

                // Reject if the valid number of vouchers doesn't add up.
                if (bookingVoucherCodesDto.Codes.Count != discountAndVouchers.Item2.Count)
                {
                    _logger.LogWarning("[AddBookingAsync] - Valid vouchers did not match vouchers submitted with booking. TenantId: {tenantId}", tenantId); // sort out logging
                    return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83709 };
                }

                if (removedVouchers.Any())
                {
                    var removedVoucherCodes = removedVouchers.Select(x => x.Code).ToList();
                    await _voucherService.RemoveVouchersFromBookingAsync(removedVoucherCodes, booking.Result.Id, tenantId, globalTenantId);
                }

                if (newlyAddedVouchers.Any())
                {
                    foreach (var newVoucher in newlyAddedVouchers)
                    {
                        await _voucherService.SnapshotVoucherForBookingAsync(newVoucher, booking.Result.Id, tenantId, false);
                        await _voucherService.IncrementUsageCountOfVoucherAsync(newVoucher, tenantId, false);
                    }
                }

                // update the discounted price on the booking record
                await _operationsBookingManager.UpdateOperationsBookingSummaryDiscountedPriceAsync(bookingReference, discountAndVouchers.Item1, tenantId);

                // update the voucher items in the booking basket.
                var basket = await _basketRepository.GetByBookingIdAsync(booking.Result.Id, tenantId);
                if (basket != null)
                {
                    _logger.LogDebug("[UpdateBookingVouchersAsync] - Updating voucherdiscount items on basket with id: {basketId}, booking id: {bookingId}, tenantId: {tenantId}...", basket.Id, basket.BookingId, tenantId);
                    var basketRes = await _basketManager.UpdateBasketVoucherDiscountItemsAsync(discountAndVouchers.Item2, basket.BasketItems.ToList(), tenantId);
                }

                var result = await CompleteUnitOfWork();

                if (!result)
                {
                    return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83852 };
                }

                // Update Customer Stripe metadata
                string metadataKey = "Package";
                string bookingDuration = DurationHelper.GetPackageDuration(assetPricing.DurationInHours, _jrnyOperationsSettings.DurationPreference);
                var externalStripeAccount = await _externalAccountRepository.GetExternalAccountByTypeAsync(booking.Result.AccountId, ExternalAccountIntegrationType.Stripe, tenantId);
                decimal priceWithUpfrontFee = assetPricing.Price + assetPricing.InitialPayment / assetPricing.InitialPaymentDurationUnits;

                string voucherDiscountInformation = $"Duration: {bookingDuration}." +
                    $"\n Mileage: {assetPricing.Mileage}." +
                    $"\n Price: {assetPricing.Price}." +
                    $"\n Price with upfront fee calculated: {priceWithUpfrontFee}.";

                if (discountAndVouchers.Item2.Count > 0)
                {
                    decimal discountedPriceWithUpfrontFee = discountAndVouchers.Item1 + assetPricing.InitialPayment / assetPricing.InitialPaymentDurationUnits;

                    List<string> codes = discountAndVouchers.Item2.Select(x => x.Code).ToList();
                    var discountSummary = await _voucherService.CalculateVoucherDiscountAsync(codes, booking.Result.AssetPackagePricingId, tenantId, globalTenantId, true);

                    voucherDiscountInformation = voucherDiscountInformation + $"\n Promotion Codes Applied: true. Codes: {string.Join(", ", codes)}." +
                        $"\n WeeklyDiscountedPrice: {discountSummary.Result.TotalWeeklyPriceWithDiscount}." +
                        $"\n Week1DiscountedPrice: {discountSummary.Result.TotalFirstWeekPriceWithDiscount}." +
                        $"\n Discounted price with upfront fee calculated: {discountedPriceWithUpfrontFee}.";
                }

                if (externalStripeAccount != null)
                {
                    _logger.LogDebug("[UpdateBookingVouchersAsync] - Found Stripe account with id: {stripeId} for accountId: {accountId}. Updating payment metadata", externalStripeAccount.ExternalId, booking.Result.AccountId);

                    var stripeUpdateResult = await _stripeService.UpdateCustomerPaymentMetadataAsync(externalStripeAccount.ExternalId, metadataKey, voucherDiscountInformation);
                    if (!stripeUpdateResult)
                    {
                        _logger.LogWarning("[UpdateBookingVouchersAsync] - Unable to update Stripe payment metadata for id: {stripeId} and accountId: {accountId}", externalStripeAccount.ExternalId, booking.Result.AccountId);
                    }
                }
                else
                {
                    _logger.LogDebug("[UpdateBookingVouchersAsync] - No external Stripe account found for accountId: {accountId}. Skipping payment metadata update", booking.Result.AccountId);
                }

                return new ApiResponse<bool>() { Success = true, Result = true, Code = OperationsApiStatusCodes.S83862 };
            }
            catch (Exception exception)
            {
                _logger.LogError("[UpdateBookingVouchersAsync] - Unexpected exception: {exception}", exception);
                return new ApiResponse<bool>() { Success = false, Result = false, Code = OperationsApiStatusCodes.E83853 };
            }

        }

        public async Task<ApiResponse<bool>> BeginBookingAuthenticationAsync(string bookingReference, string drivingLicense, Guid tenantId)
        {
            _logger.LogDebug("[BeginBookingAuthenticationAsync] - Beginning booking authentication for booking: {bookingReference}.", bookingReference);

            try
            {
                var failureResp = new ApiResponse<bool>();
                var successResp = new ApiResponse<bool>() { Success = true, Result = true, Code = OperationsApiStatusCodes.S83867 };

                // Load the booking.
                var booking = await GetBookingSummaryRecordAsync(bookingReference, tenantId);
                if (booking == null)
                {
                    failureResp.Code = OperationsApiStatusCodes.E83810;
                    return failureResp;
                }

                var account = await _accountRepository.GetByIdAsync(booking.AccountId, tenantId, true);
                if (account == null || string.IsNullOrWhiteSpace(account.MobileTelephone))
                {
                    failureResp.Code = OperationsApiStatusCodes.E83875;
                    return failureResp;
                }

                // Validate license.
                if (!account.AccountDrivers.Select(d => d.Driver).Any(l => l.DrivingLicenceValidated && l.DrivingLicence.ToLower().Equals(drivingLicense.ToLower())))
                {
                    failureResp.Code = OperationsApiStatusCodes.E83872;
                    return failureResp;
                }

                var key = ASCIIEncoding.ASCII.GetBytes(bookingReference);
                var totp = new OtpNet.Totp(key, mode: OtpNet.OtpHashMode.Sha256);
                var code = totp.ComputeTotp();

                // Add code to cache.
                _sessionService.CreateSession(Models.Enums.SessionType.TestDriveTOTP, code, bookingReference, (int)TimeSpan.FromMinutes(1000).TotalSeconds);

                if (!await _smsService.SendMessageAsync($"Your authentication code: {code}", account.MobileTelephone))
                {
                    failureResp.Code = OperationsApiStatusCodes.E83871;
                    return failureResp;
                }

                return successResp;
            }
            catch (Exception exception)
            {
                _logger.LogError("[BeginBookingAuthenticationAsync] - Unexpected Exception beginning booking authentication for booking: {bookingReference} Exception: {exception}", bookingReference, exception);

                return new ApiResponse<bool>
                {
                    Success = false,
                    Code = OperationsApiStatusCodes.E83873
                };
            }
        }

        public ApiResponse<Guid> ValidateBookingAuthenticationCode(string bookingReference, string code, Guid tenantId)
        {
            _logger.LogDebug("[ValidateBookingAuthenticationCode] - Beginning authentication of MFA code for booking: {bookingReference}.", bookingReference);

            try
            {
                var failureResp = new ApiResponse<Guid>();
                var successResp = new ApiResponse<Guid>() { Success = true, Code = OperationsApiStatusCodes.S83866 };

                var validSessionResp = _sessionService.IsSessionValid(code, Models.Enums.SessionType.TestDriveTOTP, bookingReference);

                // Check code exists.
                if (!validSessionResp.Result)
                {
                    failureResp.Code = OperationsApiStatusCodes.E83874;
                    return failureResp;
                }

                // This performs a check including the time window. For now we are just going to ensure the code exists in the cache to prevent failed validation using the method below.
                //long timeWindowUsed;
                //var key = ASCIIEncoding.ASCII.GetBytes(bookingReference);
                //var totp = new OtpNet.Totp(key, mode: OtpNet.OtpHashMode.Sha256);
                //var isValid = totp.VerifyTotp(code, out timeWindowUsed, VerificationWindow.RfcSpecifiedNetworkDelay);
                //if (!isValid)
                //{
                //    failureResp.Code = OperationsApiStatusCodes.E83866;
                //    return failureResp;
                //}

                var sessionResp = _sessionService.CreateSession(Models.Enums.SessionType.TestDrive, bookingReference, (int)TimeSpan.FromMinutes(15).TotalSeconds);

                successResp.Result = sessionResp.Result;
                return successResp;
            }
            catch (Exception exception)
            {
                _logger.LogError("[ValidateBookingAuthenticationCode] - Unexpected Exception beginning authentication of MFA code for booking: {bookingReference} Exception: {exception}", bookingReference, exception);

                return new ApiResponse<Guid>
                {
                    Success = false,
                    Code = OperationsApiStatusCodes.E83873
                };
            }
        }

        public async Task<ApiResponse<bool>> LockAsync(string bookingReference, Guid sessionId, Guid tenantId)
        {
            _logger.LogDebug("[LockAsync] - Locking vehicle for booking: {bookingReference}.", bookingReference);

            async Task<HttpResponseMessage> LockCoreAsyncLocal()
            {
                HttpClient httpClient = _httpClientFactory.CreateClient("CoreApiClient");
                httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
                var url = string.Format("{0}{1}{2}{3}", httpClient.BaseAddress, "booking/", bookingReference, "/lock");
                var response = await httpClient.PatchAsync(url, new StringContent(""));
                return response;
            }

            try
            {
                var failureResp = new ApiResponse<bool>();
                var successResp = new ApiResponse<bool> { Success = true, Result = true, Code = OperationsApiStatusCodes.S83864 };

                // Check session id.              
                var validSessionResp = _sessionService.IsSessionValid(sessionId, Models.Enums.SessionType.TestDrive, bookingReference);
                if (!validSessionResp.Result)
                {
                    failureResp.Code = OperationsApiStatusCodes.E83873;
                    return failureResp;
                }

                // Perform lock via core
                var response = await LockCoreAsyncLocal();
                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogWarning("[LockAsync] - Received status code {code} from core API when locking using booking: {bookingReference}.", response.StatusCode, bookingReference);

                    failureResp.Code = ((int)response.StatusCode).ToString();
                    return failureResp;
                }

                var responseContent = await response.Content.ReadAsStringAsync();
                var apiResponse = JsonConvert.DeserializeObject<ApiResponse<bool>>(responseContent);

                _logger.LogDebug("[LockAsync] - Response from core API when locking using booking: {bookingReference}. Response: {response}", bookingReference, apiResponse);

                if (apiResponse.Result == false)
                {
                    return apiResponse;
                }

                return successResp;
            }
            catch (Exception exception)
            {
                _logger.LogError("[LockAsync] - Unexpected Exception locking for booking: {bookingReference} Exception: {exception}", bookingReference, exception);
                return new ApiResponse<bool> { Success = false, Code = OperationsApiStatusCodes.E83838 };
            }
        }

        public async Task<ApiResponse<bool>> UnlockAsync(string bookingReference, Guid sessionId, Guid tenantId)
        {
            _logger.LogDebug("[UnlockAsync] - Unlocking vehicle for booking: {bookingReference}.", bookingReference);

            async Task<HttpResponseMessage> UnlockCoreAsyncLocal()
            {
                HttpClient httpClient = _httpClientFactory.CreateClient("CoreApiClient");
                httpClient.DefaultRequestHeaders.Add("tenantId", tenantId.ToString());
                var url = string.Format("{0}{1}{2}{3}", httpClient.BaseAddress, "booking/", bookingReference, "/unlock");
                var response = await httpClient.PatchAsync(url, new StringContent(""));
                return response;
            }

            try
            {
                var failureResp = new ApiResponse<bool>();
                var successResp = new ApiResponse<bool> { Success = true, Result = true, Code = OperationsApiStatusCodes.S83865 };

                // Check session id.             
                var validSessionResp = _sessionService.IsSessionValid(sessionId, Models.Enums.SessionType.TestDrive, bookingReference);
                if (!validSessionResp.Result)
                {
                    failureResp.Code = OperationsApiStatusCodes.E83873;
                    return failureResp;
                }

                // Perform unlock via core
                var response = await UnlockCoreAsyncLocal();
                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogWarning("[UnlockAsync] - Received status code {code} from core API when unlocking using booking: {bookingReference}.", response.StatusCode, bookingReference);

                    failureResp.Code = ((int)response.StatusCode).ToString();
                    return failureResp;
                }

                var responseContent = await response.Content.ReadAsStringAsync();
                var apiResponse = JsonConvert.DeserializeObject<ApiResponse<bool>>(responseContent);

                _logger.LogDebug("[UnlockAsync] - Response from core API when unlocking using booking: {bookingReference}. Response: {response}", bookingReference, apiResponse);

                if (apiResponse.Result == false)
                {
                    return apiResponse;
                }

                return successResp;
            }
            catch (Exception exception)
            {
                _logger.LogError("[LockAsync] - Unexpected Exception locking for booking: {bookingReference} Exception: {exception}", bookingReference, exception);
                return new ApiResponse<bool> { Success = false, Code = OperationsApiStatusCodes.E83838 };
            }
        }

        public async Task<ApiResponse<bool>> SendMobileValidationCodeAsync(string mobileNumber, Guid tenantId)
        {
            var maskedMobile = mobileNumber.Mask();

            try
            {
                var resp = new ApiResponse<bool>();
                var key = ASCIIEncoding.ASCII.GetBytes(mobileNumber);
                var totp = new Totp(key, mode: OtpHashMode.Sha256);
                var code = totp.ComputeTotp();
                var cleanMobile = Regex.Replace(mobileNumber, @"\D", "");

                // Add code to cache.
                _sessionService.CreateSession(Models.Enums.SessionType.MobileValidationTOTP, code, cleanMobile, (int)TimeSpan.FromMinutes(15).TotalSeconds);

                _logger.LogDebug("[SendMobileValidationCodeAsync] - Added mobile verification TOTP code to cache for mobile number: {number} and tenantId: {tenantId}", maskedMobile, tenantId);

                if (!await _smsService.SendMessageAsync($"Your validation code: {code}", mobileNumber))
                {
                    _logger.LogWarning("[SendMobileValidationCodeAsync] - Failed to send mobile verification TOTP code for mobile number: {number} and tenantId: {tenantId}", maskedMobile, tenantId);

                    resp.Code = OperationsApiStatusCodes.E84022;
                    return resp;
                }

                _logger.LogInformation("[SendMobileValidationCodeAsync] - Successfully sent mobile verification TOTP code to mobile number: {number} and tenantId: {tenantId}", maskedMobile, tenantId);

                resp.Success = resp.Result = true;
                return resp;
            }
            catch (Exception exception)
            {
                _logger.LogError("[SendMobileValidationCodeAsync] - Unexpected exception sending mobile validation code for mobile number: {number}. Exception: {exception}", maskedMobile, exception);

                return new ApiResponse<bool>
                {
                    Success = false,
                    Code = OperationsApiStatusCodes.E83910
                };
            }
        }

        public ApiResponse<bool> ValidateMobileValidationCode(string mobileNumber, string code, Guid tenantId)
        {
            var maskedMobile = mobileNumber.Mask();

            try
            {
                var resp = new ApiResponse<bool>();
                var cleanMobile = Regex.Replace(mobileNumber, @"\D", "");

                // Get cached code.
                var validSessionResp = _sessionService.IsSessionValid(code, Models.Enums.SessionType.MobileValidationTOTP, cleanMobile);
                if (!validSessionResp.Result)
                {
                    _logger.LogWarning("[ValidateMobileValidationCodeAsync] - Supplied code of: {suppliedCode} for mobile number: {number} and tenantId: {tenantId} is not valid",
                        code,
                        maskedMobile,
                        tenantId
                    );

                    resp.Code = OperationsApiStatusCodes.E84023;
                    return resp;
                }

                _logger.LogInformation("[ValidateMobileValidationCodeAsync] - Successfully validated mobile verification TOTP code for mobile number: {number} and tenantId: {tenantId}", maskedMobile, tenantId);

                resp.Success = resp.Result = true;
                return resp;
            }
            catch (Exception exception)
            {
                _logger.LogError("[ValidateMobileValidationCodeAsync] - Unexpected exception validating mobile validation code for mobile number: {number}. Exception: {exception}", maskedMobile, exception);

                return new ApiResponse<bool>
                {
                    Success = false,
                    Code = OperationsApiStatusCodes.E83910
                };
            }
        }



        private async Task RaiseBookingApprovedEventAsync(Booking bookingSummary, Guid assetId, Guid tenantId)
        {
            await _eventQueue.BookingApprovedEventAsync(new BookingEventData
            {
                // Event specific base data
                AccountId = bookingSummary?.AccountId ?? Guid.Empty,
                DriverId = bookingSummary?.DriverId ?? Guid.Empty,
                BookingRef = bookingSummary.UniqueReference,
                AssetId = assetId,
                TenantId = tenantId,
                Url = GetBackOfficeBookingUri(bookingSummary.UniqueReference),
                BookingId = bookingSummary.Id
            });
        }

        private async Task RaiseBookingEditedEventAsync(Booking bookingSummary, Guid assetId, Guid tenantId)
        {
            await _eventQueue.BookingEditedEventAsync(new BookingEventData
            {
                // Event specific base data
                AccountId = bookingSummary?.AccountId ?? Guid.Empty,
                DriverId = bookingSummary?.DriverId ?? Guid.Empty,
                BookingRef = bookingSummary.UniqueReference,
                AssetId = assetId,
                TenantId = tenantId,
                Url = GetBackOfficeBookingUri(bookingSummary.UniqueReference),
                BookingId = bookingSummary.Id
            });
        }

        private async Task RaiseCustomerApprovedEventAsync(Booking bookingSummary, Guid accountId, Guid tenantId)
        {
            await _eventQueue.CustomerApprovedEventAsync(new AccountEventData
            {
                AccountId = accountId,
                BookingRef = bookingSummary.UniqueReference,
                DriverId = bookingSummary.DriverId,
                Url = GetBackOfficeCustomerUri(accountId),
                TenantId = tenantId
            });
        }

        private async Task RaiseCreateJrnyIdpUserEventAsync(Guid accountId, Guid driverId, string bookingReference, string acountEmailAddress, string clientId, Guid tenantId)
        {
            await _eventQueue.CreateJrnyIdpUser(new CreateJrnyIdpUserEventData
            {
                AccountId = accountId,
                AccountUserName = acountEmailAddress,
                BookingRef = bookingReference,
                DriverId = driverId,
                ClientId = clientId,
                Url = GetBackOfficeCustomerUri(accountId),
                TenantId = tenantId
            });
        }
    }
}