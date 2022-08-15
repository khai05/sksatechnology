<?php

namespace App\Http\Controllers\Publisher;

use App\Http\Controllers\Controller;
use App\Models\Referral;
use App\Models\ReferralGroup;
use App\Models\ReferralBonusSetting;
use App\Models\ReferralBonusStack;
use App\Models\ReferralGroupProgress;
use App\Models\ExchangeRate;
use App\Models\AffiliateProperty;
use App\Models\Offer;
use App\Helpers\GenerateHelper;
use Carbon\Carbon;
use Illuminate\Http\Request;
use DB;

class RefereeEarningsReportController extends Controller
{
    public const PUBLIC_OFFER_STATUSES = [Offer::STATUS_ACTIVE];

    public function index(Request $request)
    {
        $affiliate_id = session('sess_affiliate_id');

        $highest_achievements = [
            ['name' => __('publisher_referee_earnings.signed_up'), 'isChecked' => false],
            ['name' => __('publisher_referee_earnings.1_conversion_14_days'), 'isChecked' => false],
            ['name' => __('publisher_referee_earnings.rm5_approved_earnings'), 'isChecked' => false],
            ['name' => __('publisher_referee_earnings.rm25_approved_earnings'), 'isChecked' => false],
            ['name' => __('publisher_referee_earnings.rm60_approved_earnings'), 'isChecked' => false],
            ['name' => __('publisher_referee_earnings.above_rm150_approved_earnings'), 'isChecked' => false],
        ];

        $active_property = AffiliateProperty::where('fk_status_id', '=', 1)
            ->where('fk_affiliate_id', '=', $affiliate_id)
            ->orderBy('id')
            ->first(['id', 'name']);

        $offer = Offer::query()
            ->select([
                'tbl_offer.offer_id as merchant_ia_id',
                'tbl_offer.sys_id as merchant_id',
                'tbl_offer.offer_name as merchant_name',
                'tbl_offer.preview_url',
            ])
            ->join('tbl_offer_category_relationship', 'tbl_offer_category_relationship.fk_offer_id', '=', 'tbl_offer.offer_id')
            ->join('tbl_offer_category', 'tbl_offer_category.offer_category_id', '=', 'tbl_offer_category_relationship.fk_offer_category_id')
            ->where('tbl_offer_category.is_active', '=', true)
            ->where('tbl_offer_category.offer_category_id', 18) //18 = Internal
            ->addSelect(['tbl_offer_category.offer_category_name'])
            ->where('tbl_offer.is_trd_deleted', 0)
            ->where('tbl_offer.is_display', 1)
            ->whereIn('tbl_offer.fk_offer_status_id', self::PUBLIC_OFFER_STATUSES)
            ->where('tbl_offer.offer_name', 'LIKE', '%publisher referral%')
            ->first();

        return view('publisher.reports.referee_earnings_report', compact('highest_achievements', 'active_property', 'offer'));
    }

    public function getOverviewData(Request $request)
    {
        abort_if(!$request->ajax(), 404);

        $referrer_id                = session('sess_affiliate_id');
        $reportingCurrency          = session('sess_user_defaultcurrency');

        $refEarningsQuery = Referral::selectRaw('SUM(' . strtolower($reportingCurrency) . '_approved_bonus) as total_bonus_earned, SUM(' . strtolower($reportingCurrency) . '_credited_bonus) as total_credited')
                                    ->where('referrer_affiliate_id', $referrer_id)
                                    ->first();

        $groupEarningsQuery = ReferralGroup::selectRaw('SUM(' . strtolower($reportingCurrency) . '_bonus_approved) as total_bonus_earned, SUM(' . strtolower($reportingCurrency) . '_bonus_credited) as total_credited')
                                    ->where('referrer_affiliate_id', $referrer_id)
                                    ->first();

        $activeRefQuery = Referral::where('pub_approved_date', '>=', date('Y-m-d', strtotime('-179 days')))
                                    ->where('referrer_affiliate_id', $referrer_id)
                                    ->count();

        $total_bonus_earned    = $refEarningsQuery->total_bonus_earned + $groupEarningsQuery->total_bonus_earned;
        $total_credited        = $refEarningsQuery->total_credited + $groupEarningsQuery->total_credited;

        return [
            'total_bonus_earned'    => number_format($total_bonus_earned, 2),
            'total_credited'        => number_format($total_credited, 2),
            'total_active'          => $activeRefQuery,
            'currency'              => $reportingCurrency,
        ];
    }

    public function getAjaxDataTable(Request $request)
    {
        abort_if(!$request->ajax(), 404);

        return $this->queryReferralData($request);
    }

    public function exportCSV(Request $request)
    {
        $currentTime = Carbon::now();
        $result      = $this->queryReferralData($request, true);

        $header = [
            'Referee Email',
            'Joined Date',
            'Country',
            'Milestones',
            'Bonus Earned',
            'Credited',
        ];

        array_unshift($result['data'], $header);

        $fileName = sprintf('%s.csv', "Referrals - {$currentTime->toDateString()}");

        GenerateHelper::generateCsv($result['data'], $fileName);
    }

    public function queryReferralData(Request $request, $exportCSV = false)
    {
        $draw                       = $request->input('draw');
        $start                      = $request->input('start', 0);
        $length                     = $request->input('length', 25);
        $order                      = $request->input('order');
        $search                     = $request->input('search');
        $referrer_id                = session('sess_affiliate_id'); //3239 for dummy

        $select_raw_data = [
            'email',
            'pub_approved_date',
            'referee_country',
            'milestones',
            'approved_bonus',
            'credited_bonus',
            'currency',
            'signup_affiliate_id',
            'tier_bonus_id',
            'total_achieved_stack', //total achieved stacking
        ];

        $achieved_stack = ReferralBonusStack::select('referrer_id', 'referee_id', DB::raw('COUNT(stack_enrichment_id) as total_achieved_stack'))
                                                ->where('referrer_id', $referrer_id)
                                                ->groupBy('referrer_id', 'referee_id');

        $refEarningsQuery = Referral::leftJoin('tbl_affiliate as aff', 'aff.affiliate_id', '=', 'tbl_referrals.signup_affiliate_id')
                                    ->leftJoin('tbl_user as referee', 'referee.user_id', '=', 'aff.fk_master_user_id')
                                    ->leftJoinSub($achieved_stack, 'ach_st', function ($join) {
                                        $join->on('ach_st.referee_id', 'tbl_referrals.signup_affiliate_id');
                                    })
                                    ->where('referrer_affiliate_id', $referrer_id);
        $refEarningsQuery->select($select_raw_data);

        $total_count  = $refEarningsQuery->count();
        $filter_count = $total_count;

        if (!empty($search['value'])) {
            $keyword          = $search['value'];
            $refEarningsQuery = $refEarningsQuery->where(function ($query) use ($keyword) {
                $query->orWhere('pub_approved_date', 'like', $keyword . '%')
                    ->orWhere('referee_country', 'like', $keyword . '%')
                    ->orWhere('approved_bonus', 'like', $keyword . '%')
                    ->orWhere('credited_bonus', 'like', $keyword . '%');
            });

            $filter_count = $refEarningsQuery->count();
        }

        // order by
        if (!empty($order)) {
            if (!empty($select_raw_data[$order[0]['column']])) {
                $refEarningsQuery->orderBy($select_raw_data[$order[0]['column']], $order[0]['dir']);
            }
        } else {
            $refEarningsQuery->orderBy('id', 'DESC');
        }

        if (!$exportCSV) {
            $refEarningsQuery = $refEarningsQuery->skip($start)->take($length);
        }

        $results = $refEarningsQuery->get();

        $data = [];
        foreach ($results as $result) {
            $temp_row    = [];
            $temp_row[0] = $this->encodeEmail($result->email);
            $temp_row[1] = $result->pub_approved_date;

            if ($result->referee_country == 'MY') {
                $display_country = 'Malaysia';
            } elseif ($result->referee_country == 'PH') {
                $display_country = 'Philippines';
            } elseif ($result->referee_country == 'VN') {
                $display_country = 'Vietnam';
            } elseif ($result->referee_country == 'TH') {
                $display_country = 'Thailand';
            } elseif ($result->referee_country == 'ID') {
                $display_country = 'Indonesia';
            } else {
                $display_country = $result->referee_country;
            }
            $temp_row[2] = $display_country;
            $temp_row[3] = $result->milestones;
            $temp_row[4] = $result->currency . ' ' . number_format($result->approved_bonus, 2);
            $temp_row[5] = $result->currency . ' ' . number_format($result->credited_bonus, 2);
            if (!$exportCSV) {
                $temp_row[6] = $result->signup_affiliate_id;
                $temp_row[7] = $result->tier_bonus_id;
                $temp_row[8] = $result->total_achieved_stack;
            }

            array_push($data, $temp_row);
        }

        return ['draw' => $draw, 'recordsTotal' => $total_count, 'recordsFiltered' => $filter_count, 'data' => $data];
    }

    public function getAjaxReportDetail(Request $request)
    {
        abort_if(!$request->ajax(), 404);
        $referee_id                 = $request->input('referee'); //209345
        $referrer_id                = session('sess_affiliate_id'); //3239 for dummy

        $select_raw_data = [
            'email',
            'referee_country',
            'pub_approved_date',
            'milestones',
            'referrer_signup_bonus',
            'referee_signup_bonus',
            'tier_bonus_id',
        ];

        $refEarningsQuery = Referral::leftJoin('tbl_affiliate as aff', 'aff.affiliate_id', '=', 'tbl_referrals.signup_affiliate_id')
                                    ->leftJoin('tbl_user as referee', 'referee.user_id', '=', 'aff.fk_master_user_id')
                                    ->where('referrer_affiliate_id', $referrer_id)
                            ->where('signup_affiliate_id', $referee_id);
        $refEarningsQuery->select($select_raw_data);

        $results = $refEarningsQuery->get();

        $data             = [];
        $totalBonusEarned = 0;
        foreach ($results as $result) {
            $temp_row     = [];
            $currencyName = $this->getCurrencyNameByCountry($result->referee_country);
            if ($currencyName != false && $currencyName != 'MYR') {
                $exchangeRate = ExchangeRate::getLatestCurrencyRate('MYR', $currencyName);
            } else {
                $exchangeRate = 1;
            }
            $temp_row['email']       = $this->encodeEmail($result->email);
            $referee_join_date_diff  = round((time() - strtotime($result->pub_approved_date)) / (60 * 60 * 24));
            $temp_row['joined_date'] = [
                'approved_date'=> $result->pub_approved_date,
                'status'       => $referee_join_date_diff <= 180 ? 'Active' : 'Expired',
            ];
            $temp_row['signup_bonus'] = [
                'referrer'          => number_format($result->referrer_signup_bonus, 2),
                'referrer_converted'=> $currencyName . ' ' . number_format($result->referrer_signup_bonus / $exchangeRate, 2),
                'referee'           => number_format($result->referee_signup_bonus, 2),
                'referee_converted' => $currencyName . ' ' . number_format($result->referee_signup_bonus / $exchangeRate, 2),
            ];
            $totalBonusEarned += ($result->referrer_signup_bonus / $exchangeRate);

            $tierBonuses = ReferralBonusSetting::withTrashed()
                                            ->select('level', 'bonus_value', 'threshold')
                                            ->where('country', $result->referee_country)
                                            ->whereIn('bonus_id', explode('_', $result->tier_bonus_id))
                                            ->get();

            foreach ($tierBonuses as $tierBonus) {
                $tierBonus->convertedBonusValue = $currencyName . ' ' . number_format($tierBonus->bonus_value / $exchangeRate, 2);
                if ($tierBonus->level <= $result->milestones) {
                    $tierBonus->achieved = 1;
                    $totalBonusEarned += ($tierBonus->bonus_value / $exchangeRate);
                } else {
                    $tierBonus->achieved = 0;
                }
            }
            $temp_row['milestone'] = [
                'count'=> sizeof($tierBonuses),
                'tier' => $tierBonuses,
            ];

            $achieved_stack = ReferralBonusStack::select('stack_multiplier', 'fk_bonus_id')
                                                ->where('referrer_id', $referrer_id)
                                                ->where('referee_id', $referee_id);

            $groupBonuses = ReferralBonusSetting::select('bonus_value', 'threshold')
                                                ->selectRaw('IFNULL(stack_multiplier,0) as stack_multiplier')
                                                ->leftJoinSub($achieved_stack, 'ach_st', function ($join) {
                                                    $join->on('ach_st.fk_bonus_id', 'tbl_referral_bonus_settings.bonus_id');
                                                })
                                                ->where('type', 'payout_portion')
                                                ->where('country', $result->referee_country)
                                                ->orderBy('created_at', 'asc')
                                                ->get();

            foreach ($groupBonuses as $groupBonus) {
                $groupBonus->convertedBonusValue = $currencyName . ' ' . number_format($groupBonus->bonus_value / $exchangeRate, 2);
                $totalBonusEarned += ($groupBonus->bonus_value / $exchangeRate * $groupBonus->stack_multiplier);
                if (empty($groupBonus->stack_multiplier)) {
                    $groupBonus->achieved = 0;
                } else {
                    $groupBonus->achieved = 1;
                }
            }
            $temp_row['bonus']              = $groupBonuses;
            $temp_row['total_bonus_earned'] = $currencyName . ' ' . number_format($totalBonusEarned, 2);

            array_push($data, $temp_row);
        }

        return json_encode($data);
    }

    public function getAjaxReportAdditionalBonus(Request $request)
    {
        abort_if(!$request->ajax(), 404);

        return $this->queryAdditionalBonusData($request);
    }

    public function queryAdditionalBonusData(Request $request, $exportCSV = false)
    {
        $draw                       = $request->input('draw');
        $start                      = $request->input('start', 0);
        $length                     = $request->input('length', 25);
        $order                      = $request->input('order');
        $search                     = $request->input('search');
        $referrer_id                = session('sess_affiliate_id'); //3239 for dummy

        $select_raw_data = [
            'achieved_at',
            'bonus_approved',
            'bonus_credited',
            'currency',
        ];

        $additionalBonusQuery = ReferralGroup::select($select_raw_data)
                                            ->where('referrer_affiliate_id', $referrer_id);

        $total_count  = $additionalBonusQuery->count();
        $filter_count = $total_count;

        if (!empty($search['value'])) {
            $keyword              = $search['value'];
            $additionalBonusQuery = $additionalBonusQuery->where(function ($query) use ($keyword) {
                $query->orWhere('created_at', 'like', $keyword . '%')
                    ->orWhere('bonus_approved', 'like', $keyword . '%')
                    ->orWhere('bonus_credited', 'like', $keyword . '%');
            });

            $filter_count = $additionalBonusQuery->count();
        }

        // order by
        if (!empty($order)) {
            if (!empty($select_raw_data[$order[0]['column']])) {
                $additionalBonusQuery->orderBy($select_raw_data[$order[0]['column']], $order[0]['dir']);
            }
        } else {
            $additionalBonusQuery->orderBy('id', 'DESC');
        }

        if (!$exportCSV) {
            $additionalBonusQuery = $additionalBonusQuery->skip($start)->take($length);
        }

        $results = $additionalBonusQuery->get();

        $data = [];
        foreach ($results as $result) {
            $temp_row    = [];
            $temp_row[0] = $result->achieved_at;
            $temp_row[1] = $result->currency . ' ' . number_format($result->bonus_approved, 2);
            $temp_row[2] = $result->currency . ' ' . number_format($result->bonus_credited, 2);

            array_push($data, $temp_row);
        }

        return ['draw' => $draw, 'recordsTotal' => $total_count, 'recordsFiltered' => $filter_count, 'data' => $data];
    }

    public function exportCSVAdditionalBonus(Request $request)
    {
        $currentTime = Carbon::now();
        $result      = $this->queryAdditionalBonusData($request, true);

        $header = [
            'Date',
            'Bonus Earned',
            'Credited',
        ];

        array_unshift($result['data'], $header);

        $fileName = sprintf('%s.csv', "Additional Bonus - {$currentTime->toDateString()}");

        GenerateHelper::generateCsv($result['data'], $fileName);
    }

    public function getAjaxProgressAdditionalBonus(Request $request)
    {
        abort_if(!$request->ajax(), 404);
        $referrer_id                = session('sess_affiliate_id');//3239 for dummy

        $query = ReferralGroupProgress::select('ref', 'progress', 'bonus_value', 'threshold')
                                    ->leftJoin('tbl_referral_bonus_settings', 'tbl_referral_group_progress.fk_bonus_id', '=', 'tbl_referral_bonus_settings.bonus_id')
                                    ->where('referrer_id', $referrer_id)
                                    ->where('expired', 0)
                                    ->first();

        $returnData = [];
        if (!empty($query)) {
            $returnData['total'] = $query->ref;
            if ($query->progress == 0) {
                $returnData['progress'] = 0;
            } elseif ($query->progress % $query->ref > 0) {
                $returnData['progress'] = $query->progress % $query->ref;
            } else {
                $returnData['progress'] = $query->ref;
            }
            $returnData['bonus_value']      = $query->bonus_value;
            $returnData['threshold']        = $query->threshold;
            $returnData['stack_multiplier'] = floor($query->progress / $query->ref);
        }

        return json_encode($returnData);
    }

    public function encodeEmail($email)
    {
        $pos   = strpos($email, '@');
        $array = str_split($email);
        for ($count = 2; $count <= sizeof($array) - 1; $count++) {
            if ($count < $pos || $count > $pos + 1 && $array[$count] != '.') {
                $array[$count] = '*';
            }
        }

        return implode('', $array);
    }

    public function getCurrencyNameByCountry($countryName)
    {
        if ($countryName == 'PH') {
            return 'PHP';
        } elseif ($countryName == 'VN') {
            return 'VND';
        } elseif ($countryName == 'TH') {
            return 'THB';
        } elseif ($countryName == 'ID') {
            return 'IDR';
        } elseif ($countryName == 'MY') {
            return 'MYR';
        }

        return false;
    }
}
