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
	Subscribe(ctx context.Context, in *Subscription, opts ...grpc.CallOption) (*Ack, error)
	Publish(ctx context.Context, in *Event, opts ...grpc.CallOption) (*Ack, error)
	Notify(ctx context.Context, in *Event, opts ...grpc.CallOption) (*Ack, error)
	UpdateBackup(ctx context.Context, in *Update, opts ...grpc.CallOption) (*Ack, error)
	// Fast-Delivery
	PremiumSubscribe(ctx context.Context, in *PremiumSubscription, opts ...grpc.CallOption) (*Ack, error)
	PremiumPublish(ctx context.Context, in *PremiumEvent, opts ...grpc.CallOption) (*Ack, error)
	RequestHelp(ctx context.Context, in *HelpRequest, opts ...grpc.CallOption) (*Ack, error)
	DelegateSubToHelper(ctx context.Context, in *MinimalSubData, opts ...grpc.CallOption) (*Ack, error)
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

func (c *scoutHubClient) PremiumSubscribe(ctx context.Context, in *PremiumSubscription, opts ...grpc.CallOption) (*Ack, error) {
	out := new(Ack)
	err := c.cc.Invoke(ctx, "/contentpubsub.pb.ScoutHub/PremiumSubscribe", in, out, opts...)
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

func (c *scoutHubClient) DelegateSubToHelper(ctx context.Context, in *MinimalSubData, opts ...grpc.CallOption) (*Ack, error) {
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
	Subscribe(context.Context, *Subscription) (*Ack, error)
	Publish(context.Context, *Event) (*Ack, error)
	Notify(context.Context, *Event) (*Ack, error)
	UpdateBackup(context.Context, *Update) (*Ack, error)
	// Fast-Delivery
	PremiumSubscribe(context.Context, *PremiumSubscription) (*Ack, error)
	PremiumPublish(context.Context, *PremiumEvent) (*Ack, error)
	RequestHelp(context.Context, *HelpRequest) (*Ack, error)
	DelegateSubToHelper(context.Context, *MinimalSubData) (*Ack, error)
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
func (UnimplementedScoutHubServer) PremiumSubscribe(context.Context, *PremiumSubscription) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PremiumSubscribe not implemented")
}
func (UnimplementedScoutHubServer) PremiumPublish(context.Context, *PremiumEvent) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PremiumPublish not implemented")
}
func (UnimplementedScoutHubServer) RequestHelp(context.Context, *HelpRequest) (*Ack, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestHelp not implemented")
}
func (UnimplementedScoutHubServer) DelegateSubToHelper(context.Context, *MinimalSubData) (*Ack, error) {
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
	in := new(MinimalSubData)
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
		return srv.(ScoutHubServer).DelegateSubToHelper(ctx, req.(*MinimalSubData))
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
			MethodName: "PremiumSubscribe",
			Handler:    _ScoutHub_PremiumSubscribe_Handler,
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
	Streams:  []grpc.StreamDesc{},
	Metadata: "pb/scoutHub.proto",
}
