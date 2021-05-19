// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ScoutHubClient is the client API for ScoutHub service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ScoutHubClient interface {
	// ScoutSubs
	Subscribe(ctx context.Context, in *Subscription, opts ...grpc.CallOption) (*Ack, error)
	Publish(ctx context.Context, in *Event, opts ...grpc.CallOption) (*Ack, error)
	Notify(ctx context.Context, in *Event, opts ...grpc.CallOption) (*Ack, error)
	UpdateBackup(ctx context.Context, in *Update, opts ...grpc.CallOption) (*Ack, error)
	BackupRefresh(ctx context.Context, opts ...grpc.CallOption) (ScoutHub_BackupRefreshClient, error)
	LogToTracker(ctx context.Context, in *EventLog, opts ...grpc.CallOption) (*Ack, error)
	AckToTracker(ctx context.Context, in *EventAck, opts ...grpc.CallOption) (*Ack, error)
	TrackerRefresh(ctx context.Context, in *RecruitTrackerMessage, opts ...grpc.CallOption) (*Ack, error)
	AckUp(ctx context.Context, in *EventAck, opts ...grpc.CallOption) (*Ack, error)
	ResendEvent(ctx context.Context, opts ...grpc.CallOption) (ScoutHub_ResendEventClient, error)
	AckOp(ctx context.Context, in *Ack, opts ...grpc.CallOption) (*Ack, error)
	// FastDelivery
	AdvertiseGroup(ctx context.Context, in *AdvertRequest, opts ...grpc.CallOption) (*Ack, error)
	GroupSearchRequest(ctx context.Context, in *SearchRequest, opts ...grpc.CallOption) (*SearchReply, error)
	PremiumSubscribe(ctx context.Context, in *PremiumSubscription, opts ...grpc.CallOption) (*Ack, error)
	PremiumUnsubscribe(ctx context.Context, in *PremiumSubscription, opts ...grpc.CallOption) (*Ack, error)
	PremiumPublish(ctx context.Context, in *PremiumEvent, opts ...grpc.CallOption) (*Ack, error)
	RequestHelp(ctx context.Context, in *HelpRequest, opts ...grpc.CallOption) (*Ack, error)
	DelegateSubToHelper(ctx context.Context, in *DelegateSub, opts ...grpc.CallOption) (*Ack, error)
}

type scoutHubClient struct {
	cc grpc.ClientConnInterface
}

func NewScoutHubClient(cc grpc.ClientConnInterface) ScoutHubClient {
	return &scoutHubClient{cc}
}

func (c *scoutHubClient) Subscribe(ctx context.Context, in *Subscription, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/Subscribe", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) Publish(ctx context.Context, in *Event, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/Publish", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) Notify(ctx context.Context, in *Event, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/Notify", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) UpdateBackup(ctx context.Context, in *Update, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/UpdateBackup", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) BackupRefresh(ctx context.Context, opts ...grpc.CallOption) (ScoutHub_BackupRefreshClient, error) {
	stream, err := c.cc.NewStream(ctx, &ScoutHub_ServiceDesc.Streams[0], "/contentpubsub.pb.ScoutHub/BackupRefresh", opts...)
	if err != nil {
		return nil, err
	}
	x := &scoutHubBackupRefreshClient{stream}
	return x, nil
}

type ScoutHub_BackupRefreshClient interface {
	Send(*Update) error
	CloseAndRecv() (*Ack, error)
	grpc.ClientStream
}

type scoutHubBackupRefreshClient struct {
	grpc.ClientStream
}

func (x *scoutHubBackupRefreshClient) Send(m *Update) error {
	return x.ClientStream.SendMsg(m)
}

func (x *scoutHubBackupRefreshClient) CloseAndRecv() (*Ack, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(Ack)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *scoutHubClient) LogToTracker(ctx context.Context, in *EventLog, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/LogToTracker", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) AckToTracker(ctx context.Context, in *EventAck, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/AckToTracker", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) TrackerRefresh(ctx context.Context, in *RecruitTrackerMessage, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/TrackerRefresh", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) AckUp(ctx context.Context, in *EventAck, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/AckUp", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) ResendEvent(ctx context.Context, opts ...grpc.CallOption) (ScoutHub_ResendEventClient, error) {
	stream, err := c.cc.NewStream(ctx, &ScoutHub_ServiceDesc.Streams[1], "/contentpubsub.pb.ScoutHub/ResendEvent", opts...)
	if err != nil {
		return nil, err
	}
	x := &scoutHubResendEventClient{stream}
	return x, nil
}

type ScoutHub_ResendEventClient interface {
	Send(*EventLog) error
	CloseAndRecv() (*Ack, error)
	grpc.ClientStream
}

type scoutHubResendEventClient struct {
	grpc.ClientStream
}

func (x *scoutHubResendEventClient) Send(m *EventLog) error {
	return x.ClientStream.SendMsg(m)
}

func (x *scoutHubResendEventClient) CloseAndRecv() (*Ack, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(Ack)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *scoutHubClient) AckOp(ctx context.Context, in *Ack, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/AckOp", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) AdvertiseGroup(ctx context.Context, in *AdvertRequest, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/AdvertiseGroup", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) GroupSearchRequest(ctx context.Context, in *SearchRequest, opts ...grpc.CallOption) (*SearchReply, error) {
	out := new(SearchReply)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/GroupSearchRequest", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) PremiumSubscribe(ctx context.Context, in *PremiumSubscription, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/PremiumSubscribe", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) PremiumUnsubscribe(ctx context.Context, in *PremiumSubscription, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/PremiumUnsubscribe", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) PremiumPublish(ctx context.Context, in *PremiumEvent, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/PremiumPublish", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) RequestHelp(ctx context.Context, in *HelpRequest, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/RequestHelp", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *scoutHubClient) DelegateSubToHelper(ctx context.Context, in *DelegateSub, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/DelegateSubToHelper", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ScoutHubServer is the server API for ScoutHub service.
// All implementations must embed UnimplementedScoutHubServer
// for forward compatibility
type ScoutHubServer interface {
	// ScoutSubs
	Subscribe(context.Context, *Subscription) (*Ack, error)
	Publish(context.Context, *Event) (*Ack, error)
	Notify(context.Context, *Event) (*Ack, error)
	UpdateBackup(context.Context, *Update) (*Ack, error)
	BackupRefresh(ScoutHub_BackupRefreshServer) error
	LogToTracker(context.Context, *EventLog) (*Ack, error)
	AckToTracker(context.Context, *EventAck) (*Ack, error)
	TrackerRefresh(context.Context, *RecruitTrackerMessage) (*Ack, error)
	AckUp(context.Context, *EventAck) (*Ack, error)
	ResendEvent(ScoutHub_ResendEventServer) error
	AckOp(context.Context, *Ack) (*Ack, error)
	// FastDelivery
	AdvertiseGroup(context.Context, *AdvertRequest) (*Ack, error)
	GroupSearchRequest(context.Context, *SearchRequest) (*SearchReply, error)
	PremiumSubscribe(context.Context, *PremiumSubscription) (*Ack, error)
	PremiumUnsubscribe(context.Context, *PremiumSubscription) (*Ack, error)
	PremiumPublish(context.Context, *PremiumEvent) (*Ack, error)
	RequestHelp(context.Context, *HelpRequest) (*Ack, error)
	DelegateSubToHelper(context.Context, *DelegateSub) (*Ack, error)
	mustEmbedUnimplementedScoutHubServer()
}

// UnimplementedScoutHubServer must be embedded to have forward compatible implementations.
type UnimplementedScoutHubServer struct {
}

func (UnimplementedScoutHubServer) Subscribe(context.Context, *Subscription) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Subscribe not implemented")
}
func (UnimplementedScoutHubServer) Publish(context.Context, *Event) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Publish not implemented")
}
func (UnimplementedScoutHubServer) Notify(context.Context, *Event) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Notify not implemented")
}
func (UnimplementedScoutHubServer) UpdateBackup(context.Context, *Update) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateBackup not implemented")
}
func (UnimplementedScoutHubServer) BackupRefresh(ScoutHub_BackupRefreshServer) error {
	return status.Errorf(codes.Unimplemented, "method BackupRefresh not implemented")
}
func (UnimplementedScoutHubServer) LogToTracker(context.Context, *EventLog) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LogToTracker not implemented")
}
func (UnimplementedScoutHubServer) AckToTracker(context.Context, *EventAck) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AckToTracker not implemented")
}
func (UnimplementedScoutHubServer) TrackerRefresh(context.Context, *RecruitTrackerMessage) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TrackerRefresh not implemented")
}
func (UnimplementedScoutHubServer) AckUp(context.Context, *EventAck) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AckUp not implemented")
}
func (UnimplementedScoutHubServer) ResendEvent(ScoutHub_ResendEventServer) error {
	return status.Errorf(codes.Unimplemented, "method ResendEvent not implemented")
}
func (UnimplementedScoutHubServer) AckOp(context.Context, *Ack) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AckOp not implemented")
}
func (UnimplementedScoutHubServer) AdvertiseGroup(context.Context, *AdvertRequest) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AdvertiseGroup not implemented")
}
func (UnimplementedScoutHubServer) GroupSearchRequest(context.Context, *SearchRequest) (*SearchReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GroupSearchRequest not implemented")
}
func (UnimplementedScoutHubServer) PremiumSubscribe(context.Context, *PremiumSubscription) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PremiumSubscribe not implemented")
}
func (UnimplementedScoutHubServer) PremiumUnsubscribe(context.Context, *PremiumSubscription) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PremiumUnsubscribe not implemented")
}
func (UnimplementedScoutHubServer) PremiumPublish(context.Context, *PremiumEvent) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PremiumPublish not implemented")
}
func (UnimplementedScoutHubServer) RequestHelp(context.Context, *HelpRequest) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestHelp not implemented")
}
func (UnimplementedScoutHubServer) DelegateSubToHelper(context.Context, *DelegateSub) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DelegateSubToHelper not implemented")
}
func (UnimplementedScoutHubServer) mustEmbedUnimplementedScoutHubServer() {}

// UnsafeScoutHubServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ScoutHubServer will
// result in compilation errors.
type UnsafeScoutHubServer interface {
	mustEmbedUnimplementedScoutHubServer()
}

func RegisterScoutHubServer(s grpc.ServiceRegistrar, srv ScoutHubServer) {
	s.RegisterService(&ScoutHub_ServiceDesc, srv)
}

func _ScoutHub_Subscribe_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Subscription)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).Subscribe(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/Subscribe",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).Subscribe(ctx, req.(*Subscription))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_Publish_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Event)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).Publish(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/Publish",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).Publish(ctx, req.(*Event))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_Notify_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Event)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).Notify(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/Notify",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).Notify(ctx, req.(*Event))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_UpdateBackup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Update)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).UpdateBackup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/UpdateBackup",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).UpdateBackup(ctx, req.(*Update))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_BackupRefresh_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(ScoutHubServer).BackupRefresh(&scoutHubBackupRefreshServer{stream})
}

type ScoutHub_BackupRefreshServer interface {
	SendAndClose(*Ack) error
	Recv() (*Update, error)
	grpc.ServerStream
}

type scoutHubBackupRefreshServer struct {
	grpc.ServerStream
}

func (x *scoutHubBackupRefreshServer) SendAndClose(m *Ack) error {
	return x.ServerStream.SendMsg(m)
}

func (x *scoutHubBackupRefreshServer) Recv() (*Update, error) {
	m := new(Update)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _ScoutHub_LogToTracker_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EventLog)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).LogToTracker(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/LogToTracker",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).LogToTracker(ctx, req.(*EventLog))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_AckToTracker_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EventAck)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).AckToTracker(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/AckToTracker",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).AckToTracker(ctx, req.(*EventAck))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_TrackerRefresh_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RecruitTrackerMessage)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).TrackerRefresh(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/TrackerRefresh",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).TrackerRefresh(ctx, req.(*RecruitTrackerMessage))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_AckUp_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EventAck)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).AckUp(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/AckUp",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).AckUp(ctx, req.(*EventAck))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_ResendEvent_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(ScoutHubServer).ResendEvent(&scoutHubResendEventServer{stream})
}

type ScoutHub_ResendEventServer interface {
	SendAndClose(*Ack) error
	Recv() (*EventLog, error)
	grpc.ServerStream
}

type scoutHubResendEventServer struct {
	grpc.ServerStream
}

func (x *scoutHubResendEventServer) SendAndClose(m *Ack) error {
	return x.ServerStream.SendMsg(m)
}

func (x *scoutHubResendEventServer) Recv() (*EventLog, error) {
	m := new(EventLog)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _ScoutHub_AckOp_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Ack)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).AckOp(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/AckOp",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).AckOp(ctx, req.(*Ack))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_AdvertiseGroup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AdvertRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).AdvertiseGroup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/AdvertiseGroup",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).AdvertiseGroup(ctx, req.(*AdvertRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_GroupSearchRequest_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SearchRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).GroupSearchRequest(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/GroupSearchRequest",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).GroupSearchRequest(ctx, req.(*SearchRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_PremiumSubscribe_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PremiumSubscription)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).PremiumSubscribe(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/PremiumSubscribe",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).PremiumSubscribe(ctx, req.(*PremiumSubscription))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_PremiumUnsubscribe_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PremiumSubscription)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).PremiumUnsubscribe(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/PremiumUnsubscribe",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).PremiumUnsubscribe(ctx, req.(*PremiumSubscription))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_PremiumPublish_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PremiumEvent)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).PremiumPublish(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/PremiumPublish",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).PremiumPublish(ctx, req.(*PremiumEvent))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_RequestHelp_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HelpRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).RequestHelp(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/RequestHelp",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).RequestHelp(ctx, req.(*HelpRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ScoutHub_DelegateSubToHelper_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DelegateSub)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ScoutHubServer).DelegateSubToHelper(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/contentpubsub.pb.ScoutHub/DelegateSubToHelper",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ScoutHubServer).DelegateSubToHelper(ctx, req.(*DelegateSub))
	}
	return interceptor(ctx, in, info, handler)
}

// ScoutHub_ServiceDesc is the grpc.ServiceDesc for ScoutHub service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ScoutHub_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "contentpubsub.pb.ScoutHub",
	HandlerType: (*ScoutHubServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Subscribe",
			Handler:    _ScoutHub_Subscribe_Handler,
		},
		{
			MethodName: "Publish",
			Handler:    _ScoutHub_Publish_Handler,
		},
		{
			MethodName: "Notify",
			Handler:    _ScoutHub_Notify_Handler,
		},
		{
			MethodName: "UpdateBackup",
			Handler:    _ScoutHub_UpdateBackup_Handler,
		},
		{
			MethodName: "LogToTracker",
			Handler:    _ScoutHub_LogToTracker_Handler,
		},
		{
			MethodName: "AckToTracker",
			Handler:    _ScoutHub_AckToTracker_Handler,
		},
		{
			MethodName: "TrackerRefresh",
			Handler:    _ScoutHub_TrackerRefresh_Handler,
		},
		{
			MethodName: "AckUp",
			Handler:    _ScoutHub_AckUp_Handler,
		},
		{
			MethodName: "AckOp",
			Handler:    _ScoutHub_AckOp_Handler,
		},
		{
			MethodName: "AdvertiseGroup",
			Handler:    _ScoutHub_AdvertiseGroup_Handler,
		},
		{
			MethodName: "GroupSearchRequest",
			Handler:    _ScoutHub_GroupSearchRequest_Handler,
		},
		{
			MethodName: "PremiumSubscribe",
			Handler:    _ScoutHub_PremiumSubscribe_Handler,
		},
		{
			MethodName: "PremiumUnsubscribe",
			Handler:    _ScoutHub_PremiumUnsubscribe_Handler,
		},
		{
			MethodName: "PremiumPublish",
			Handler:    _ScoutHub_PremiumPublish_Handler,
		},
		{
			MethodName: "RequestHelp",
			Handler:    _ScoutHub_RequestHelp_Handler,
		},
		{
			MethodName: "DelegateSubToHelper",
			Handler:    _ScoutHub_DelegateSubToHelper_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "BackupRefresh",
			Handler:       _ScoutHub_BackupRefresh_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "ResendEvent",
			Handler:       _ScoutHub_ResendEvent_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "pb/scoutHub.proto",
}
